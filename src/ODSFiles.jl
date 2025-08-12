"""
# ODSFiles.jl

**Read and write OpenDocument Spreadsheet (ODS) files in Julia**

ODSFiles.jl provides a comprehensive interface for working with ODS files, supporting both simple operations on individual sheets and advanced operations on multi-sheet workbooks with metadata preservation.

## Quick Start

```julia
using ODSFiles

# Read a single sheet
df = read_sheet("data.ods")

# Write a single sheet
write_sheet("output.ods", df, "MySheet")

# Read all sheets with metadata
specs = read_sheets("workbook.ods")

# Write multiple sheets
write_sheets("output.ods", specs)
```

## Core Types

### `ODSFile`
Represents an opened ODS file for reading operations. Automatically parses the file structure and makes sheets available for data extraction.

### `SheetSpec`
A structure that holds both data and metadata for a sheet:
- `name::String`: Sheet name
- `data::DataFrame`: The actual data
- `position::Union{Int, Nothing}`: Sheet position (1-based)
- `include_headers::Bool`: Whether headers are present
- `data_start_row::Int`: Row where data begins (1-indexed)

## Reading Functions

### Single Sheet Operations

**`read_sheet(filepath; sheet=nothing, header=true, types=true, skipto=1)`**

Read data from a single sheet into a DataFrame.

- `filepath`: Path to ODS file or `ODSFile` object
- `sheet`: Sheet name (String), index (Int), or `nothing` for first sheet
- `header`: Whether first row contains column headers
- `types`: Auto-detect types (`true`), keep as strings (`false`), or specify with `Dict`
- `skipto`: Start reading from this row (1-indexed)

```julia
# Read first sheet with auto-detection
df = read_sheet("data.ods")

# Read specific sheet with custom types
df = read_sheet("sales.ods", sheet="Q1", 
                types=Dict("Revenue" => Float64, "Region" => String))

# Skip header rows and start from row 3
df = read_sheet("report.ods", skipto=3, header=false)
```

**`get_sheet_names(filepath)`**

Get all sheet names in an ODS file.

```julia
names = get_sheet_names("workbook.ods")  # ["Sheet1", "Sales", "Products"]
```

### Multi-Sheet Operations

**`read_sheets(filepath; sheets=nothing, header=true, types=true, skipto=1)`**

Read multiple sheets and return as `SheetSpec` objects with preserved metadata.

- `sheets`: Vector of sheet names/indices, or `nothing` for all sheets

```julia
# Read all sheets
specs = read_sheets("workbook.ods")

# Read specific sheets
specs = read_sheets("data.ods", sheets=["Sales", "Products"])
specs = read_sheets("data.ods", sheets=[1, 3])  # by index

# Access data and metadata
for spec in specs
    println("Sheet '\$(spec.name)' has \$(nrow(spec.data)) rows")
    println("Headers included: \$(spec.include_headers)")
end
```

**`read_all_sheets(filepath; header=true, types=true, skipto=1)`**

Read all sheets into a simple dictionary mapping names to DataFrames.

```julia
sheets = read_all_sheets("workbook.ods")
sales_data = sheets["Sales"]
product_data = sheets["Products"]
```

## Writing Functions

**`write_sheet(filepath, data, sheet_name="Sheet1"; overwrite=true)`**

Write a single DataFrame to an ODS file.

```julia
df = DataFrame(Name=["Alice", "Bob"], Age=[25, 30])
write_sheet("people.ods", df, "Employees")
```

**`write_sheets(filepath, data; overwrite=true)`**

Write multiple sheets to an ODS file. Accepts several input formats:

```julia
# Dictionary format
sheets = Dict("Sales" => sales_df, "Products" => products_df)
write_sheets("workbook.ods", sheets)

# SheetSpec format (full control)
specs = [
    SheetSpec("Summary", summary_df, position=1),
    SheetSpec("Details", details_df, position=2, include_headers=false)
]
write_sheets("report.ods", specs)

# Name => DataFrame pairs
pairs = ["Q1" => q1_df, "Q2" => q2_df, "Q3" => q3_df]
write_sheets("quarterly.ods", pairs)
```

## Advanced Usage

### Round-trip Operations with Metadata Preservation

```julia
# Read file with all metadata
specs = read_sheets("input.ods")

# Modify data while preserving structure
specs[1] = SheetSpec(
    specs[1].name, 
    transform(specs[1].data, :Revenue => x -> x * 1.1),  # 10% increase
    position=specs[1].position,
    include_headers=specs[1].include_headers
)

# Write back with original structure preserved
write_sheets("updated.ods", specs)
```

### Custom Type Conversion

```julia
# Specify exact types for columns
types = Dict(
    "ID" => Int,
    "Name" => String, 
    "Revenue" => Float64,
    "Active" => Bool
)

df = read_sheet("data.ods", types=types)
```

### Working with Complex Files

```julia
# Open file once, read multiple sheets efficiently
ods = ODSFile("large_workbook.ods")
sheet_names = get_sheet_names(ods)

# Read only sheets matching a pattern
financial_sheets = filter(name -> contains(name, "Finance"), sheet_names)
specs = read_sheets(ods, sheets=financial_sheets)
```

### Sheet Positioning

```julia
# Control exact sheet order in output file
specs = [
    SheetSpec("Executive Summary", summary_df, position=1),
    SheetSpec("Raw Data", raw_df, position=3),
    SheetSpec("Analysis", analysis_df, position=2)
]
write_sheets("report.ods", specs)  # Order: Summary, Analysis, Raw Data
```

## Error Handling

The package provides clear error messages for common issues:

- **File not found**: `ArgumentError` with file path
- **Invalid sheet names**: Lists available sheets  
- **Sheet index out of range**: Shows valid range
- **Duplicate sheet positions**: Reports conflicting positions
- **Type conversion failures**: Warns and continues with strings

## Performance Notes

- For large files, create an `ODSFile` object once and reuse it
- Use `types=false` to skip type detection for faster parsing
- SheetSpec format enables efficient round-trip operations
- Memory usage is proportional to data size, not file size

## Compatibility

- Supports ODS format version 1.3
- Compatible with LibreOffice, Apache OpenOffice, and other ODS-compliant applications  
- Handles empty cells, merged cells, and repeated cell ranges
- Preserves numeric precision and data types

## Examples

### Data Analysis Workflow

```julia
using ODSFiles, DataFrames

# Load multi-sheet financial data
specs = read_sheets("financial_data.ods")

# Process each sheet
results = map(specs) do spec
    # Calculate summary statistics
    summary = combine(spec.data, 
        :Revenue => sum => :total_revenue,
        :Customers => length => :customer_count
    )
    
    # Return new SheetSpec with processed data
    SheetSpec("\$(spec.name)_Summary", summary, 
             position=spec.position,
             include_headers=true)
end

# Save processed results
write_sheets("financial_summary.ods", results)
```

### Template-based Reporting

```julia
# Read template structure
template_specs = read_sheets("report_template.ods")

# Fill with new data while preserving formatting metadata
filled_specs = map(template_specs) do spec
    if spec.name == "Data"
        # Replace data but keep metadata
        SheetSpec(spec.name, new_data_df,
                 position=spec.position,
                 include_headers=spec.include_headers)
    else
        spec  # Keep other sheets unchanged
    end
end

write_sheets("monthly_report.ods", filled_specs)
```
"""
module ODSFiles

using ZipFile
using EzXML
using DataFrames
using Dates

# Version and metadata
const VERSION = v"1.0.0"

# Include submodules and utilities
include("core/types.jl")
include("core/validation.jl")
include("io/xml_utils.jl")
include("io/reader.jl")
include("io/writer.jl")
include("processing/type_conversion.jl")
include("processing/data_extraction.jl")

# Re-export main types and functions
export ODSFile, SheetSpec
export read_sheet, read_sheets, read_all_sheets, get_sheet_names
export write_sheet, write_sheets

end # module ODSFiles