"""
Core Types for ODSFiles.jl
src/core/types.jl
"""

"""
    ODSFile

A struct representing an ODS file for reading operations.
Contains the parsed XML content and available sheets.

# Fields
- `filepath::String`: Path to the ODS file
- `content::String`: Raw XML content
- `root::EzXML.Node`: Parsed XML root node
- `sheets::Dict{String, EzXML.Node}`: Dictionary mapping sheet names to XML nodes

# Constructor
    ODSFile(filepath::String)

Load an ODS file for reading operations.

# Example
```julia
ods = ODSFile("data.ods")
sheet_names = get_sheet_names(ods)
df = read_sheet(ods, "Sheet1")
```
"""
struct ODSFile
    filepath::String
    content::String
    root::EzXML.Node
    sheets::Dict{String,EzXML.Node}

    function ODSFile(filepath::String)
        if !isfile(filepath)
            throw(ArgumentError("File not found: $filepath"))
        end

        # Open the ODS file as a ZIP archive
        zip_reader = ZipFile.Reader(filepath)

        # Find and read the content.xml file
        content_xml = nothing
        for file in zip_reader.files
            if file.name == "content.xml"
                content_xml = String(read(file))
                break
            end
        end
        close(zip_reader)

        if content_xml === nothing
            throw(ArgumentError("Invalid ODS file: content.xml not found"))
        end

        # Parse the XML
        doc = parsexml(content_xml)
        root = doc.root

        # Define namespaces for XPath queries
        namespace_map = Dict(
            "office" => "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
            "table" => "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
            "text" => "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
        )

        # Find all table elements (sheets) and create a mapping
        tables = findall("//table:table", root, namespace_map)
        sheets = Dict{String,EzXML.Node}()

        for (i, table) in enumerate(tables)
            name_attr = findfirst("@table:name", table, namespace_map)
            sheet_name = name_attr !== nothing ? nodecontent(name_attr) : "Sheet$i"
            sheets[sheet_name] = table
        end

        new(filepath, content_xml, root, sheets)
    end
end

"""
    SheetSpec

Structure to hold sheet configuration for reading and writing operations.

# Fields
- `name::String`: Name of the sheet
- `data::DataFrame`: DataFrame containing the data
- `position::Union{Int, Nothing}`: Position of the sheet (1-based), or nothing for automatic ordering
- `include_headers::Bool`: Whether the data includes/should include column headers
- `data_start_row::Int`: Row number where data starts (1-based, after headers if present)

# Constructor
    SheetSpec(name::String, data::DataFrame; position::Union{Int, Nothing}=nothing, include_headers::Bool=true, data_start_row::Int=1)

# Example
```julia
# For writing
df = DataFrame(A=[1,2], B=[3,4])
spec = SheetSpec("MySheet", df, position=1, include_headers=true)

# For reading (returned by read_sheets)
specs = read_sheets("data.ods")
```
"""
struct SheetSpec
    name::String
    data::DataFrame
    position::Union{Int,Nothing}
    include_headers::Bool
    data_start_row::Int

    SheetSpec(
        name::String,
        data::DataFrame;
        position::Union{Int,Nothing} = nothing,
        include_headers::Bool = true,
        data_start_row::Int = 1,
    ) = new(name, data, position, include_headers, data_start_row)
end