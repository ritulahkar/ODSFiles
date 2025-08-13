"""
XML utilities for ODSFiles.jl
src/io/xml_utils.jl
"""

# Standard ODS namespaces - using const for compile-time optimization
const ODS_NAMESPACES = Dict(
    "office" => "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table" => "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text" => "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "style" => "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "fo" => "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "meta" => "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "manifest" => "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"
)

# Pre-computed namespace values for frequent access
const OFFICE_NS = ODS_NAMESPACES["office"]
const TABLE_NS = ODS_NAMESPACES["table"]
const TEXT_NS = ODS_NAMESPACES["text"]
const STYLE_NS = ODS_NAMESPACES["style"]
const FO_NS = ODS_NAMESPACES["fo"]

# Pre-computed attribute strings to avoid repeated concatenation
const OFFICE_VALUE_TYPE = "office:value-type"
const OFFICE_VALUE = "office:value"
const OFFICE_BOOLEAN_VALUE = "office:boolean-value"
const OFFICE_DATE_VALUE = "office:date-value"
const OFFICE_VERSION = "office:version"

# Type constants for faster comparisons
const TYPE_STRING = "string"
const TYPE_FLOAT = "float"
const TYPE_BOOLEAN = "boolean"
const TYPE_DATE = "date"
const VERSION_1_3 = "1.3"

"""
    create_content_root_element()

Create the root element for content.xml with proper namespaces.
"""
function create_content_root_element()
    root = ElementNode("office:document-content")
    
    # Use pre-computed constants for faster attribute setting
    root["xmlns:office"] = OFFICE_NS
    root["xmlns:table"] = TABLE_NS
    root["xmlns:text"] = TEXT_NS
    root["xmlns:style"] = STYLE_NS
    root["xmlns:fo"] = FO_NS
    root[OFFICE_VERSION] = VERSION_1_3
    
    return root
end

"""
    extract_cell_value(cell_node, namespace_map)

Extract the text value from a table cell node preserving multiple empty rows and newlines.
"""
function extract_cell_value(cell_node, namespace_map)
    # Try to find text content in paragraphs
    text_nodes = findall(".//text:p", cell_node, namespace_map)

    if !isempty(text_nodes)
        # Pre-allocate vector for better performance
        text_parts = Vector{String}(undef, length(text_nodes))
        
        @inbounds for i in eachindex(text_nodes)
            content = nodecontent(text_nodes[i])
            if !isnothing(content)
                # Don't strip whitespace - preserve it as is
                text_parts[i] = content
            else
                # Preserve empty paragraphs as empty strings
                text_parts[i] = ""
            end
        end
        
        # Join all parts with newlines, preserving multiple empty lines
        return join(text_parts, "\n")
    end

    # If no text paragraphs, try direct content
    content = nodecontent(cell_node)
    return isnothing(content) ? "" : content  # Don't strip - preserve whitespace
end

"""
    create_text_cell(text::AbstractString)

Create a table cell containing text with optimized node creation.
"""
@inline function create_text_cell(text::AbstractString)
    cell = ElementNode("table:table-cell")
    cell[OFFICE_VALUE_TYPE] = TYPE_STRING

    # Handle multi-line text by creating multiple paragraphs
    lines = split(text, '\n')
    
    if length(lines) == 1
        # Single line - create one paragraph
        paragraph = ElementNode("text:p")
        text_node = TextNode(text)
        link!(paragraph, text_node)
        link!(cell, paragraph)
    else
        # Multiple lines - create paragraph for each line (including empty ones)
        for line in lines
            paragraph = ElementNode("text:p")
            text_node = TextNode(line)  # Don't strip - preserve empty lines
            link!(paragraph, text_node)
            link!(cell, paragraph)
        end
    end

    return cell
end

"""
    create_typed_cell(value)

Create a table cell with appropriate type based on the value using optimized type dispatch.
"""
function create_typed_cell(value)
    # Handle missing values early
    if ismissing(value) || value === nothing
        return ElementNode("table:table-cell")
    end

    # Dispatch to specialized methods for better performance
    return create_typed_cell_dispatch(value)
end

# Specialized methods for different value types to improve performance
@inline function create_typed_cell_dispatch(value::Bool)
    cell = ElementNode("table:table-cell")
    cell[OFFICE_VALUE_TYPE] = TYPE_BOOLEAN
    cell[OFFICE_BOOLEAN_VALUE] = value ? "true" : "false"  # Avoid string() call
    
    paragraph = ElementNode("text:p")
    text_node = TextNode(value ? "true" : "false")
    link!(paragraph, text_node)
    link!(cell, paragraph)
    
    return cell
end

@inline function create_typed_cell_dispatch(value::Integer)
    cell = ElementNode("table:table-cell")
    cell[OFFICE_VALUE_TYPE] = TYPE_FLOAT
    
    # Convert to float string directly for better performance
    float_val = Float64(value)
    float_str = string(float_val)
    cell[OFFICE_VALUE] = float_str
    
    paragraph = ElementNode("text:p")
    text_node = TextNode(string(value))  # Display as integer
    link!(paragraph, text_node)
    link!(cell, paragraph)
    
    return cell
end

@inline function create_typed_cell_dispatch(value::AbstractFloat)
    cell = ElementNode("table:table-cell")
    cell[OFFICE_VALUE_TYPE] = TYPE_FLOAT
    cell[OFFICE_VALUE] = format_float_value_optimized(value)
    
    paragraph = ElementNode("text:p")
    text_node = TextNode(format_float_value_optimized(value))
    link!(paragraph, text_node)
    link!(cell, paragraph)
    
    return cell
end

@inline function create_typed_cell_dispatch(value::Union{Date, DateTime})
    cell = ElementNode("table:table-cell")
    cell[OFFICE_VALUE_TYPE] = TYPE_DATE
    
    date_str = string(value)
    cell[OFFICE_DATE_VALUE] = date_str
    
    paragraph = ElementNode("text:p")
    text_node = TextNode(date_str)
    link!(paragraph, text_node)
    link!(cell, paragraph)
    
    return cell
end

# Generic fallback for other types
function create_typed_cell_dispatch(value)
    cell = ElementNode("table:table-cell")
    str_value = string(value)
    
    # Try to parse as number for better type detection
    if is_parseable_number_optimized(str_value)
        parsed_value = parse(Float64, str_value)
        cell[OFFICE_VALUE_TYPE] = TYPE_FLOAT
        cell[OFFICE_VALUE] = format_float_value_optimized(parsed_value)
    else
        cell[OFFICE_VALUE_TYPE] = TYPE_STRING
    end

    # Handle multi-line strings in generic dispatch too
    lines = split(str_value, '\n')
    
    if length(lines) == 1
        paragraph = ElementNode("text:p")
        text_node = TextNode(str_value)
        link!(paragraph, text_node)
        link!(cell, paragraph)
    else
        for line in lines
            paragraph = ElementNode("text:p")
            text_node = TextNode(line)
            link!(paragraph, text_node)
            link!(cell, paragraph)
        end
    end

    return cell
end

"""
    is_parseable_number_optimized(s::AbstractString)

Optimized check if a string can be parsed as a number with early returns.
"""
@inline function is_parseable_number_optimized(s::AbstractString)
    s = strip(s)  # Remove leading/trailing whitespace
    length(s) == 0 && return false

    # Quick character-based pre-check
    first_char = s[1]
    
    # Fast path for simple integers
    if all(isdigit, s) || 
       (length(s) > 1 && (first_char == '-' || first_char == '+') && all(isdigit, s[2:end]))
        return true
    end

    # Scientific notation or special float values
    if occursin(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$", s) ||
       lowercase(s) in ["inf", "-inf", "+inf", "infinity", "-infinity", "+infinity", "nan"]
        return true
    end
    
    # Full parsing fallback
    try
        parse(Float64, s)
        return true
    catch
        return false
    end
end

"""
    format_float_value_optimized(value::AbstractFloat)

Optimized float formatting with pre-computed special case handling.
"""
@inline function format_float_value_optimized(value::AbstractFloat)
    isnan(value) && return "NaN"
    isinf(value) && return signbit(value) ? "-Inf" : "Inf"
    return string(value)
end

# Batch processing utilities for better performance with large datasets

"""
    create_text_cells_batch(texts::AbstractVector{<:AbstractString})

Create multiple text cells efficiently with batch processing.
"""
function create_text_cells_batch(texts::AbstractVector{<:AbstractString})
    cells = Vector{typeof(ElementNode("table:table-cell"))}(undef, length(texts))
    
    @inbounds for i in eachindex(texts)
        cells[i] = create_text_cell(texts[i])
    end
    
    return cells
end

"""
    create_typed_cells_batch(values::AbstractVector)

Create multiple typed cells efficiently with batch processing and type-stable operations.
"""
function create_typed_cells_batch(values::AbstractVector)
    cells = Vector{typeof(ElementNode("table:table-cell"))}(undef, length(values))
    
    @inbounds for i in eachindex(values)
        cells[i] = create_typed_cell(values[i])
    end
    
    return cells
end

"""
    extract_cell_values_batch(cell_nodes::AbstractVector, namespace_map)

Extract values from multiple cell nodes efficiently with batch processing.
"""
function extract_cell_values_batch(cell_nodes::AbstractVector, namespace_map)
    values = Vector{String}(undef, length(cell_nodes))
    
    @inbounds for i in eachindex(cell_nodes)
        values[i] = extract_cell_value(cell_nodes[i], namespace_map)
    end
    
    return values
end

# Memory pool for frequently used elements (advanced optimization)

"""
    ElementPool

A simple pool for reusing ElementNode objects to reduce allocation pressure.
"""
mutable struct ElementPool
    text_cells::Vector{Any}  # Pool of reusable text cell templates
    max_size::Int
    
    ElementPool(max_size::Int = 100) = new(Vector{Any}(), max_size)
end

const GLOBAL_ELEMENT_POOL = ElementPool()

"""
    get_pooled_text_cell(pool::ElementPool, text::AbstractString)

Get a text cell from the pool or create a new one if pool is empty.
"""
function get_pooled_text_cell(pool::ElementPool, text::AbstractString)
    if !isempty(pool.text_cells)
        cell = pop!(pool.text_cells)
        # Update text content - handle multi-line case
        if occursin('\n', text)
            # Clear existing paragraphs and recreate
            empty!(children(cell))
            lines = split(text, '\n')
            for line in lines
                paragraph = ElementNode("text:p")
                text_node = TextNode(line)
                link!(paragraph, text_node)
                link!(cell, paragraph)
            end
        else
            # Single line - update existing paragraph
            paragraph = first(children(cell))
            text_node = first(children(paragraph))
            text_node.content = text
        end
        return cell
    else
        return create_text_cell(text)
    end
end

"""
    return_to_pool(pool::ElementPool, cell)

Return a cell to the pool for reuse.
"""
function return_to_pool(pool::ElementPool, cell)
    if length(pool.text_cells) < pool.max_size
        push!(pool.text_cells, cell)
    end
end

# Legacy compatibility functions (maintain old interface)

"""
    is_parseable_number(s::AbstractString)

Legacy compatibility wrapper for the optimized number parsing function.
"""
is_parseable_number(s::AbstractString) = is_parseable_number_optimized(s)

"""
    format_float_value(value::AbstractFloat)

Legacy compatibility wrapper for the optimized float formatting function.
"""
format_float_value(value::AbstractFloat) = format_float_value_optimized(value)