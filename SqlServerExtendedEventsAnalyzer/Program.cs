using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;

XDocument doc;
using (var connection = new SqlConnection())
{
    await connection.OpenAsync();

    using var command = new SqlCommand(@"select target_data
from sys.dm_xe_session_targets
join sys.dm_xe_sessions on dm_xe_session_targets.event_session_address = dm_xe_sessions.address
	and dm_xe_sessions.name = 'Test'
where dm_xe_session_targets.target_name='ring_buffer'", connection);
    using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

    await reader.ReadAsync();
    doc = XDocument.Load(reader.GetTextReader(0));
}

var root = doc.Element("RingBufferTarget")!;
var events = root.Elements("event").ToArray();

var dataAttributesToKeep = new HashSet<string>
{
    "showplan_xml",
};

XNamespace showplan = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

var results = root
    .Elements("event")
    .Select(ev =>
    {
        var subset = new
        {
            Data = ev
                .Elements("data")
                .Select(data => new
                {
                    Data = data,
                    Name = data.Attribute("name")?.Value,
                })
                .Where(data => data.Name != null)
                .ToDictionary(data => data.Name, data => data.Data),
            Actions = ev
                .Elements("action")
                .Select(data => new
                {
                    Data = data,
                    Name = data.Attribute("name")?.Value,
                })
                .Where(data => data.Name != null)
                .ToDictionary(data => data.Name, data => data.Data),
        };

        var plan = subset.Data["showplan_xml"].Element("value")!.Element(showplan + "ShowPlanXML")!;

        subset.Data.Remove("showplan_xml");

        var hash = ulong.Parse(subset.Actions["query_plan_hash"].Element("value")!.Value); // Sometimes this is 0
        var sql_text = subset.Actions["sql_text"].Element("value")?.Value;

        subset.Actions.Remove("query_plan_hash");

        return new
        {
            //Data = subset.Data,
            //Actions = subset.Actions,
            Hash = hash,
            Plan = plan,
            Text = sql_text,
        };
    })
    .Where(info => info.Hash != 0 && (info.Text == null || !info.Text.Contains("sys.")) && //Exclude system queries
        (info.Plan.Descendants().SelectMany(element => element.Attributes()).Any(attribute => attribute.Value.Contains("Index Scan"))
        ||
        (info.Text != null && info.Text.Contains("jd.PassThroughJob = 0 AND"))
        ||
        info.Plan.Descendants(showplan + "IndexScan").Any(element => element.Attribute("Lookup")?.Value == "1") // Key Lookup
        )
        )
    .GroupBy(info => info.Hash)
    .ToDictionary(group => group.Key, group => group.First());

var tasks = new List<Task>();
foreach (var kv in results)
{
    var key = kv.Key;
    var value = kv.Value;

    var parameters = value
        .Plan
        .Descendants(showplan + "QueryPlan")
        .Elements(showplan + "ParameterList")
        .Elements(showplan + "ColumnReference")
        .Select(column => $"{column.Attribute("Column")?.Value} {column.Attribute("ParameterDataType")?.Value} = {column.Attribute("ParameterRuntimeValue")?.Value}")
        .ToArray();

    var name = $"Plans/{key}.sqlplan";
    if (File.Exists(name))
        continue;

    tasks.Add(File.WriteAllTextAsync(name, value.Plan.ToString()));

    var text = value.Text;

    if (text == null)
        continue;

    var newline = text.Contains("\r") ? "\r\n" : "\n";

    var queryPrefix = parameters.Length == 0 ? null : $"DECLARE{newline}{string.Join("," + newline, parameters)};{newline}";

    tasks.Add(File.WriteAllTextAsync($"Plans/{key}.sql", queryPrefix + text));
}

await Task.WhenAll(tasks);

tasks.Clear();