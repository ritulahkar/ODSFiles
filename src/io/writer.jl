"""
Writing functions for ODSFiles.jl
src/io/writer.jl
"""

"""
    write_sheet(filepath::String, data::DataFrame, sheet_name::String="Sheet1"; overwrite::Bool=true)

Write a single DataFrame to an ODS file.
"""
function write_sheet(
    filepath::String,
    data::DataFrame,
    sheet_name::String = "Sheet1";
    overwrite::Bool = true,
)
    sheet_spec = SheetSpec(sheet_name, data)
    write_sheets(filepath, [sheet_spec]; overwrite = overwrite)
end

"""
    write_sheets(filepath::String, data; overwrite::Bool=true)

Write multiple sheets to an ODS file. Supports multiple input formats.
"""
function write_sheets(
    filepath::String,
    data::Dict{String,DataFrame};
    overwrite::Bool = true,
)
    sheet_specs = [SheetSpec(name, df) for (name, df) in data]
    write_sheets(filepath, sheet_specs; overwrite = overwrite)
end

function write_sheets(
    filepath::String,
    data::Vector{Pair{String,DataFrame}};
    overwrite::Bool = true,
)
    sheet_specs = [SheetSpec(name, df) for (name, df) in data]
    write_sheets(filepath, sheet_specs; overwrite = overwrite)
end

function write_sheets(filepath::String, data::Vector{SheetSpec}; overwrite::Bool = true)
    validate_file_path(filepath, overwrite)

    # Sort sheets by position if specified
    ordered_sheets = sort_sheets_by_position(data)

    # Generate the complete ODS file structure
    content_xml = generate_spreadsheet_content(ordered_sheets)
    manifest_xml = generate_manifest_content()
    meta_xml = generate_metadata_content()
    styles_xml = generate_styles_content()
    mimetype_content = "application/vnd.oasis.opendocument.spreadsheet"

    # Write all components to ZIP archive
    write_ods_archive(
        filepath,
        content_xml,
        manifest_xml,
        meta_xml,
        styles_xml,
        mimetype_content,
    )

    # Print success message
    print_write_summary(filepath, ordered_sheets)
end

"""
    generate_spreadsheet_content(sheets::Vector{SheetSpec})

Generate the content.xml file for the spreadsheet.
"""
function generate_spreadsheet_content(sheets::Vector{SheetSpec})
    doc = XMLDocument()
    root = create_content_root_element()
    setroot!(doc, root)

    # Build document structure
    body = ElementNode("office:body")
    spreadsheet = ElementNode("office:spreadsheet")
    link!(root, body)
    link!(body, spreadsheet)

    # Add each sheet as a table
    for sheet in sheets
        table_node = create_sheet_table(sheet)
        link!(spreadsheet, table_node)
    end

    return string(doc)
end

"""
    create_sheet_table(sheet::SheetSpec)

Create a table element for a single sheet.
"""
function create_sheet_table(sheet::SheetSpec)
    table = ElementNode("table:table")
    table["table:name"] = sheet.name

    # Handle empty DataFrames
    if isempty(sheet.data) || ncol(sheet.data) == 0
        empty_row = ElementNode("table:table-row")
        empty_cell = ElementNode("table:table-cell")
        link!(empty_row, empty_cell)
        link!(table, empty_row)
        return table
    end

    # Add header row if requested
    if sheet.include_headers
        header_row = create_header_row(names(sheet.data))
        link!(table, header_row)
    end

    # Add data rows
    for row_idx = 1:nrow(sheet.data)
        data_row = create_data_row(sheet.data, row_idx)
        link!(table, data_row)
    end

    return table
end

"""
    create_header_row(column_names::Vector{String})

Create a header row with column names.
"""
function create_header_row(column_names::Vector{String})
    header_row = ElementNode("table:table-row")

    for col_name in column_names
        cell = create_text_cell(string(col_name))
        link!(header_row, cell)
    end

    return header_row
end

"""
    create_data_row(df::DataFrame, row_idx::Int)

Create a data row from a DataFrame row.
"""
function create_data_row(df::DataFrame, row_idx::Int)
    data_row = ElementNode("table:table-row")

    for col_name in names(df)
        cell_value = df[row_idx, col_name]
        cell = create_typed_cell(cell_value)
        link!(data_row, cell)
    end

    return data_row
end

"""
    write_ods_archive(filepath, content_xml, manifest_xml, meta_xml, styles_xml, mimetype_content)

Write all components to a ZIP archive.
"""
function write_ods_archive(
    filepath::String,
    content_xml::String,
    manifest_xml::String,
    meta_xml::String,
    styles_xml::String,
    mimetype_content::String,
)
    zip_writer = ZipFile.Writer(filepath)

    try
        # Write mimetype first (ODS specification requirement)
        f = ZipFile.addfile(zip_writer, "mimetype")
        write(f, mimetype_content)

        # Write main content
        f = ZipFile.addfile(zip_writer, "content.xml")
        write(f, content_xml)

        # Write metadata
        f = ZipFile.addfile(zip_writer, "META-INF/manifest.xml")
        write(f, manifest_xml)

        f = ZipFile.addfile(zip_writer, "meta.xml")
        write(f, meta_xml)

        f = ZipFile.addfile(zip_writer, "styles.xml")
        write(f, styles_xml)

    finally
        close(zip_writer)
    end
end

"""
    generate_manifest_content()

Generate the manifest.xml file content.
"""
function generate_manifest_content()
    return """<?xml version="1.0" encoding="UTF-8"?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.3">
    <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>
    <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
    <manifest:file-entry manifest:full-path="styles.xml" manifest:media-type="text/xml"/>
    <manifest:file-entry manifest:full-path="meta.xml" manifest:media-type="text/xml"/>
</manifest:manifest>"""
end

"""
    generate_metadata_content()

Generate the meta.xml file content.
"""
function generate_metadata_content()
    timestamp = string(now())
    return """<?xml version="1.0" encoding="UTF-8"?>
<office:document-meta xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" 
                      xmlns:meta="urn:oasis:names:tc:opendocument:xmlns:meta:1.0" 
                      office:version="1.3">
    <office:meta>
        <meta:generator>ODSFiles.jl v$(VERSION)</meta:generator>
        <meta:creation-date>$timestamp</meta:creation-date>
    </office:meta>
</office:document-meta>"""
end

"""
    generate_styles_content()

Generate the styles.xml file content.
"""
function generate_styles_content()
    return """<?xml version="1.0" encoding="UTF-8"?>
<office:document-styles xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" 
                        xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0" 
                        xmlns:fo="urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0" 
                        office:version="1.3">
    <office:styles>
        <style:default-style style:family="table-cell">
            <style:table-cell-properties style:decimal-places="2" 
                                       style:text-align-source="fix" 
                                       style:repeat-content="false"/>
            <style:text-properties style:font-name="Arial" 
                                 style:font-size="10pt"/>
        </style:default-style>
    </office:styles>
    <office:automatic-styles/>
    <office:master-styles/>
</office:document-styles>"""
end

"""
    print_write_summary(filepath::String, sheets::Vector{SheetSpec})

Print a summary of the write operation.
"""
function print_write_summary(filepath::String, sheets::Vector{SheetSpec})
    println("✓ ODS file exported successfully: $filepath")
    println("  Sheets: $(length(sheets))")
    for (i, sheet) in enumerate(sheets)
        headers_info = sheet.include_headers ? "with headers" : "no headers"
        println(
            "  [$i] '$(sheet.name)': $(nrow(sheet.data)) rows × $(ncol(sheet.data)) cols ($headers_info)",
        )
    end
end