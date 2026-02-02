using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Text;

/// <summary>
/// 피벗 테이블 캐시의 SharedItems를 분석하여 CSV로 변환하는 클래스입니다.
/// </summary>
public class PivotSharedItemsFlattener
{
    /// <summary>
    /// 지정된 피벗 캐시 정의 파트에서 모든 필드의 SharedItems를 CSV 문자열로 변환합니다.
    /// 각 필드는 별도의 CSV 문자열로 생성되어 Dictionary에 저장됩니다.
    /// </summary>
    /// <param name="cacheDefinitionPart">분석할 피벗 테이블 캐시 정의 파트입니다.</param>
    /// <returns>필드 이름을 키로, 해당 필드의 SharedItems를 나타내는 CSV 문자열을 값으로 하는 Dictionary입니다.</returns>
    public Dictionary<string, string> FlattenAllFields(PivotTableCacheDefinitionPart cacheDefinitionPart)
    {
        var allFieldsCsv = new Dictionary<string, string>();
        if (cacheDefinitionPart.PivotCacheDefinition?.CacheFields == null)
        {
            return allFieldsCsv;
        }

        foreach (CacheField field in cacheDefinitionPart.PivotCacheDefinition.CacheFields)
        {
            if (field?.SharedItems != null)
            {
                string fieldName = field.Name?.Value ?? $"UnnamedField_{allFieldsCsv.Count}";
                var flattenedRows = new List<List<string>>();
                
                // 재귀 함수 호출 시작
                FlattenItemsRecursive(field.SharedItems.ChildElements, new List<string>(), flattenedRows);
                
                if (flattenedRows.Count > 0)
                {
                    allFieldsCsv[fieldName] = ConvertToCsv(flattenedRows);
                }
            }
        }

        return allFieldsCsv;
    }

    /// <summary>
    /// OpenXmlElement 목록(SharedItems 또는 GroupItem의 자식)을 재귀적으로 탐색하여 평탄화된 데이터 행을 생성합니다.
    /// </summary>
    /// <param name="items">분석할 OpenXmlElement의 열거형입니다.</param>
    /// <param name="currentPath">현재까지의 그룹 경로입니다. (예: ["2023", "1분기"])</param>
    /// <param name="flattenedRows">결과 행이 추가될 리스트입니다.</param>
    private void FlattenItemsRecursive(IEnumerable<OpenXmlElement> items, List<string> currentPath, List<List<string>> flattenedRows)
    {
        foreach (var item in items)
        {
            // GroupItem인 경우, 현재 경로에 그룹 이름을 추가하고 재귀적으로 더 깊이 탐색합니다.
            if (item is GroupItem groupItem)
            {
                var newPath = new List<string>(currentPath);
                // 그룹 이름은 StringItem, NumberItem 등 다양한 타입일 수 있습니다.
                // GroupItem.Member.Name은 그룹의 이름이 아니라 개별 멤버의 이름을 나타내므로,
                // 여기서는 GroupItem 바로 아래에 있는 첫 번째 아이템의 값을 그룹 이름으로 간주합니다.
                // 실제 Excel의 동작 방식에 따라 조정이 필요할 수 있습니다.
                string groupName = GetItemValue(groupItem.Elements().FirstOrDefault()); 
                newPath.Add(groupName);
                
                // 그룹의 멤버들을 대상으로 재귀 호출 (Member 요소 건너뛰고 실제 아이템들을 전달)
                FlattenItemsRecursive(groupItem.Elements<Member>().SelectMany(m => m.Elements()), newPath, flattenedRows);
            }
            // Member는 GroupItem 내부에 있는 아이템을 감싸는 컨테이너입니다.
            // 하지만 SharedItems 아래 Member는 보통 StringItem 등을 직접 포함하지 않고 GroupItem을 포함합니다.
            // 위 GroupItem 처리 로직에서 Elements<Member>().SelectMany(m => m.Elements()) 로 처리되도록 수정했습니다.
            // 여기서는 GroupItem이 아닌 단일 아이템으로서 Member가 나타날 경우를 대비합니다.
            else if (item is Member member)
            {
                // Member 요소는 GroupItem 내부에 있거나 필드 자체의 멤버일 수 있습니다.
                // 여기서는 단일 멤버의 값을 추출합니다.
                var finalRow = new List<string>(currentPath);
                finalRow.Add(member.Name?.Value ?? GetItemValue(member.Elements().FirstOrDefault()));
                flattenedRows.Add(finalRow);
            }
            // GroupItem이 아닌 다른 모든 아이템(StringItem, NumberItem 등)은 최종 값(리프 노드)으로 처리합니다.
            else if (item is StringItem || item is NumberItem || item is DateTimeItem)
            {
                var finalRow = new List<string>(currentPath);
                finalRow.Add(GetItemValue(item));
                flattenedRows.Add(finalRow);
            }
        }
    }

    /// <summary>
    /// 다양한 아이템 타입(StringItem, NumberItem, DateTimeItem 등)에서 실제 값을 문자열로 추출합니다.
    /// </summary>
    private string GetItemValue(OpenXmlElement? item)
    {
        return item switch
        {
            StringItem s => s.Val?.Value ?? "",
            NumberItem n => n.Val?.Value ?? "",
            DateTimeItem d => d.Val?.Value ?? "",
            _ => ""
        };
    }

    /// <summary>
    /// 평탄화된 행들의 리스트를 단일 CSV 문자열로 변환합니다.
    /// </summary>
    private string ConvertToCsv(List<List<string>> rows)
    {
        var sb = new StringBuilder();
        
        // 헤더 생성 (예: Group1, Group2, Value)
        int maxColumns = rows.Count > 0 ? rows.Max(r => r.Count) : 0;
        if (maxColumns > 0)
        {
            for (int i = 0; i < maxColumns -1; i++)
            {
                sb.Append($"Group{i + 1},");
            }
            sb.AppendLine("Value");
        }

        // 데이터 행 추가
        foreach (var row in rows)
        {
            for (int i = 0; i < row.Count; i++)
            {
                string value = row[i];
                // CSV 형식에 맞게 값에 쉼표나 큰따옴표가 포함된 경우 처리
                if (value.Contains(",") || value.Contains("\"") || value.Contains("\n")) // 줄바꿈도 고려
                {
                    sb.Append($"\"{value.Replace("\"", "\"\")}\"");
                }
                else
                {
                    sb.Append(value);
                }
                if (i < row.Count - 1)
                {
                    sb.Append(",");
                }
            }
            sb.AppendLine();
        }
        return sb.ToString();
    }
}

/*
/// <summary>
/// 메인 프로그램 클래스 (실행을 위해 필요)
/// </summary>
public class Program
{
    public static void Main(string[] args)
    {
        // 이 코드를 사용하려면 먼저 Excel 파일을 열고 PivotTableCacheDefinitionPart를 가져와야 합니다.
        // 예를 들어, 프로젝트와 같은 디렉토리에 "PivotSample.xlsx" 파일을 두고 실행할 수 있습니다.
        string filePath = "PivotSample.xlsx"; // 실제 Excel 파일 경로로 변경하세요.

        if (!System.IO.File.Exists(filePath))
        {
            Console.WriteLine($"Error: Excel file not found at '{filePath}'. Please create a sample Excel file with a PivotTable.");
            Console.WriteLine("Expected Excel Structure:");
            Console.WriteLine("1. Create a sheet with some data, e.g.:");
            Console.WriteLine("   Date       | Category | Sales");
            Console.WriteLine("   2023-01-01 | A        | 100");
            Console.WriteLine("   2023-01-02 | B        | 150");
            Console.WriteLine("   2023-02-01 | A        | 120");
            Console.WriteLine("   2024-01-01 | C        | 200");
            Console.WriteLine("2. Insert a PivotTable based on this data.");
            Console.WriteLine("3. For grouped items, drag a date field (e.g., 'Date') to Rows, then right-click on the date field in the PivotTable and select 'Group'. Group by 'Years' and 'Months'.");
            Console.WriteLine("4. Save the Excel file as 'PivotSample.xlsx' in the same directory as the executable.");
            return;
        }

        try
        {
            using (SpreadsheetDocument spreadsheetDocument = SpreadsheetDocument.Open(filePath, false))
            {
                var workbookPart = spreadsheetDocument.WorkbookPart;
                if (workbookPart == null)
                {
                    Console.WriteLine("WorkbookPart not found.");
                    return;
                }

                // 통합 문서의 첫 번째 피벗 캐시를 가져옵니다.
                // 실제 시나리오에서는 적절한 피벗 캐시를 선택해야 할 수 있습니다.
                PivotTableCacheDefinitionPart cacheDefPart = workbookPart.PivotTableCacheDefinitionParts.FirstOrDefault();
                if (cacheDefPart == null)
                {
                    Console.WriteLine("PivotTableCacheDefinitionPart not found in the workbook.");
                    return;
                }
                
                var flattener = new PivotSharedItemsFlattener();
                Dictionary<string, string> results = flattener.FlattenAllFields(cacheDefPart);

                // 결과를 출력하거나 파일로 저장합니다.
                if (results.Count == 0)
                {
                    Console.WriteLine("No SharedItems found or processed in the PivotTable cache.");
                }
                else
                {
                    foreach (var entry in results)
                    {
                        string outputFileName = $"{entry.Key}_shared_items.csv";
                        System.IO.File.WriteAllText(outputFileName, entry.Value, Encoding.UTF8);
                        Console.WriteLine($"--- Field: '{entry.Key}' --- SharedItems saved to '{outputFileName}'");
                        // Console.WriteLine(entry.Value); // 콘솔에 직접 출력하려면 이 주석을 해제하세요.
                    }
                    Console.WriteLine("\nProcessing complete. Check the generated CSV files.");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
    }
}
*/
