"""
XML utilities for ODSFiles.jl
src/io/xml_utils.jl
"""

# Standard ODS namespaces
const ODS_NAMESPACES = Dict(
    "office" => "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table" => "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text" => "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "style" => "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "fo" => "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "meta" => "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "manifest" => "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"
)

"""
    create_content_root_element()

Create the root element for content.xml with proper namespaces.
"""
function create_content_root_element()
    root = ElementNode("office:document-content")
    root["xmlns:office"] = ODS_NAMESPACES["office"]
    root["xmlns:table"] = ODS_NAMESPACES["table"]
    root["xmlns:text"] = ODS_NAMESPACES["text"]
    root["xmlns:style"] = ODS_NAMESPACES["style"]
    root["xmlns:fo"] = ODS_NAMESPACES["fo"]
    root["office:version"] = "1.3"
    return root
end

"""
    extract_cell_value(cell_node, namespace_map)

Extract the text value from a table cell node.
"""
function extract_cell_value(cell_node, namespace_map)
    # Try to find text content in paragraphs
    text_nodes = findall(".//text:p", cell_node, namespace_map)

    if !isempty(text_nodes)
        # Concatenate all text paragraphs
        text_parts = String[]
        for text_node in text_nodes
            content = nodecontent(text_node)
            if !isnothing(content)
                push!(text_parts, strip(content))
            end
        end
        return join(text_parts, "\n")
    end

    # If no text paragraphs, try direct content
    content = nodecontent(cell_node)
    return isnothing(content) ? "" : strip(content)
end

"""
    create_text_cell(text::AbstractString)

Create a table cell containing text.
"""
function create_text_cell(text::AbstractString)
    cell = ElementNode("table:table-cell")
    cell["office:value-type"] = "string"

    paragraph = ElementNode("text:p")
    text_node = TextNode(string(text))
    link!(paragraph, text_node)
    link!(cell, paragraph)

    return cell
end

"""
    create_typed_cell(value)

Create a table cell with appropriate type based on the value.
"""
function create_typed_cell(value)
    cell = ElementNode("table:table-cell")

    if ismissing(value) || value === nothing
        # Return empty cell for missing values
        return cell
    end

    str_value = string(value)

    # Type detection and cell configuration
    if isa(value, Bool)
        cell["office:value-type"] = "boolean"
        cell["office:boolean-value"] = lowercase(str_value)
    elseif isa(value, Integer)
        cell["office:value-type"] = "float"
        cell["office:value"] = string(Float64(value))
    elseif isa(value, AbstractFloat)
        cell["office:value-type"] = "float"
        cell["office:value"] = format_float_value(value)
    elseif isa(value, Date) || isa(value, DateTime)
        cell["office:value-type"] = "date"
        cell["office:date-value"] = string(value)
    elseif is_parseable_number(str_value)
        try
            parsed_value = parse(Float64, str_value)
            cell["office:value-type"] = "float"
            cell["office:value"] = format_float_value(parsed_value)
        catch
            cell["office:value-type"] = "string"
        end
    else
        cell["office:value-type"] = "string"
    end

    # Add text content
    paragraph = ElementNode("text:p")
    text_node = TextNode(str_value)
    link!(paragraph, text_node)
    link!(cell, paragraph)

    return cell
end

"""
    is_parseable_number(s::AbstractString)

Check if a string can be parsed as a number.
"""
function is_parseable_number(s::AbstractString)
    s = strip(s)
    isempty(s) && return false

    try
        parse(Float64, s)
        return true
    catch
        return false
    end
end

"""
    format_float_value(value::AbstractFloat)

Format a float value for XML output, handling special cases.
"""
function format_float_value(value::AbstractFloat)
    if isnan(value)
        return "NaN"
    elseif isinf(value)
        return value > 0 ? "Inf" : "-Inf"
    else
        return string(value)
    end
end