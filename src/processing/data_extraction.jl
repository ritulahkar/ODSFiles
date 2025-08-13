"""
Data extraction utilities for ODSFiles.jl
src/processing/data_extraction.jl
"""

"""
    extract_table_data(table_node; header::Bool=true, skipto::Int=1)

Extract data from a table XML node into a DataFrame with optimized processing.
Properly handles multiple empty rows and repeated row elements.
"""
function extract_table_data(table_node; header::Bool = true, skipto::Int = 1)
    # Find all rows including repeated ones
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)

    if isempty(rows) || skipto > length(rows)
        return DataFrame()
    end

    # Expand repeated rows to get the actual row structure
    expanded_rows = expand_repeated_rows(rows)
    
    if isempty(expanded_rows) || skipto > length(expanded_rows)
        return DataFrame()
    end

    # Pre-compute row range for better performance
    row_range = skipto:length(expanded_rows)
    n_rows = length(row_range)
    
    # Pre-allocate data structures
    data_matrix = Vector{Vector{String}}(undef, n_rows)
    max_cols = 0

    # Extract data from all rows starting from skipto with optimized processing
    @inbounds for (idx, row_pos) in enumerate(row_range)
        row_info = expanded_rows[row_pos]
        row_data = extract_row_data_with_repeats(row_info)
        
        max_cols = max(max_cols, length(row_data))
        data_matrix[idx] = row_data
    end

    # Early return for empty data
    if max_cols == 0
        return DataFrame()
    end

    # Pad rows to have the same number of columns using optimized approach
    pad_rows_to_max_cols!(data_matrix, max_cols)

    # Handle headers and create DataFrame
    return create_dataframe_optimized(data_matrix, header, max_cols)
end

"""
    expand_repeated_rows(rows)

Expand table:table-row elements that have table:number-rows-repeated attribute
to preserve multiple empty rows.
"""
function expand_repeated_rows(rows)
    expanded_rows = []
    
    @inbounds for row in rows
        # Check for row repetition
        repeat_count = get_row_repeat_count(row)
        
        if repeat_count == 1
            push!(expanded_rows, (node = row, is_repeated = false))
        else
            # Add the same row multiple times to preserve empty rows
            for _ in 1:repeat_count
                push!(expanded_rows, (node = row, is_repeated = true))
            end
        end
    end
    
    return expanded_rows
end

"""
    get_row_repeat_count(row_node)

Get the number of times a row should be repeated based on table:number-rows-repeated attribute.
"""
@inline function get_row_repeat_count(row_node)
    repeat_attr = findfirst("@table:number-rows-repeated", row_node, ODS_NAMESPACES)
    repeat_attr === nothing && return 1
    
    content = nodecontent(repeat_attr)
    isnothing(content) && return 1
    
    # Fast parsing with early return for common case
    if content == "1"
        return 1
    end
    
    try
        parsed = parse(Int, content)
        return parsed > 0 ? parsed : 1
    catch
        return 1
    end
end

"""
    extract_row_data_with_repeats(row_info)

Extract data from a row, handling both individual rows and repeated row structures.
"""
@inline function extract_row_data_with_repeats(row_info)
    row = row_info.node
    cells = findall(".//table:table-cell", row, ODS_NAMESPACES)
    return extract_row_data_optimized(cells)
end

"""
    extract_row_data_optimized(cells)

Optimized extraction of data from a row's cells with minimal allocations.
Properly handles repeated cells to preserve multiple empty columns.
"""
@inline function extract_row_data_optimized(cells)
    if isempty(cells)
        return String[]
    end
    
    # Pre-allocate with estimated size (accounting for potential cell repeats)
    row_data = String[]
    estimated_size = length(cells) * 2  # Conservative estimate for repeated cells
    sizehint!(row_data, estimated_size)

    @inbounds for cell in cells
        # Handle repeated cells efficiently
        repeat_count = get_cell_repeat_count_optimized(cell)
        cell_value = extract_cell_value(cell, ODS_NAMESPACES)

        # Add repeated values with minimal overhead
        if repeat_count == 1
            push!(row_data, cell_value)
        else
            append!(row_data, fill(cell_value, repeat_count))
        end
    end

    return row_data
end

"""
    get_cell_repeat_count_optimized(cell_node)

Optimized function to get cell repeat count with caching and early returns.
"""
@inline function get_cell_repeat_count_optimized(cell_node)
    repeat_attr = findfirst("@table:number-columns-repeated", cell_node, ODS_NAMESPACES)
    repeat_attr === nothing && return 1
    
    content = nodecontent(repeat_attr)
    isnothing(content) && return 1
    
    # Fast parsing with early return for common case
    if content == "1"
        return 1
    end
    
    try
        parsed = parse(Int, content)
        return parsed > 0 ? parsed : 1
    catch
        return 1
    end
end

"""
    pad_rows_to_max_cols!(data_matrix::Vector{Vector{String}}, max_cols::Int)

In-place padding of rows to ensure consistent column count.
"""
@inline function pad_rows_to_max_cols!(data_matrix::Vector{Vector{String}}, max_cols::Int)
    @inbounds for row_data in data_matrix
        current_len = length(row_data)
        if current_len < max_cols
            # Use resize! for better performance than repeated push!
            resize!(row_data, max_cols)
            # Fill the new elements with empty strings
            @inbounds for i in (current_len + 1):max_cols
                row_data[i] = ""
            end
        end
    end
end

"""
    create_dataframe_optimized(data_matrix::Vector{Vector{String}}, header::Bool, max_cols::Int)

Optimized DataFrame creation with pre-allocated data structures.
"""
function create_dataframe_optimized(data_matrix::Vector{Vector{String}}, header::Bool, max_cols::Int)
    if header && !isempty(data_matrix)
        # Extract headers with optimized processing
        headers = make_unique_headers_optimized(data_matrix[1])
        data_rows = length(data_matrix) > 1 ? view(data_matrix, 2:length(data_matrix)) : Vector{String}[]
    else
        # Pre-allocate header names
        headers = Vector{String}(undef, max_cols)
        @inbounds for i in 1:max_cols
            headers[i] = "Column$i"
        end
        data_rows = data_matrix
    end

    # Optimized DataFrame construction using pre-allocated columns
    return create_dataframe_from_columns(headers, data_rows, max_cols)
end

"""
    create_dataframe_from_columns(headers::Vector{String}, data_rows, max_cols::Int)

Create DataFrame with pre-allocated columns for better performance.
"""
function create_dataframe_from_columns(headers::Vector{String}, data_rows, max_cols::Int)
    n_data_rows = length(data_rows)
    
    # Pre-allocate all columns
    columns = Vector{Vector{String}}(undef, max_cols)
    
    @inbounds for col_idx in 1:max_cols
        column = Vector{String}(undef, n_data_rows)
        @inbounds for row_idx in 1:n_data_rows
            row_data = data_rows[row_idx]
            column[row_idx] = col_idx <= length(row_data) ? row_data[col_idx] : ""
        end
        columns[col_idx] = column
    end
    
    # Create DataFrame with pre-built columns
    return DataFrame(columns, headers, copycols=false)
end

"""
    make_unique_headers_optimized(headers::Vector{String})

Optimized function to ensure all headers are unique with minimal string allocations.
"""
function make_unique_headers_optimized(headers::Vector{String})
    n_headers = length(headers)
    unique_headers = Vector{String}(undef, n_headers)
    header_counts = Dict{String,Int}()
    sizehint!(header_counts, n_headers)

    @inbounds for i in eachindex(headers)
        header = headers[i]
        clean_header = isempty(strip(header)) ? "Column" : strip(header)

        count = get!(header_counts, clean_header, 0)
        if count == 0
            unique_headers[i] = clean_header
            header_counts[clean_header] = 1
        else
            header_counts[clean_header] = count + 1
            unique_headers[i] = string(clean_header, "_", count + 1)
        end
    end

    return unique_headers
end

"""
    extract_sheet_metadata_optimized(table_node)

Optimized metadata extraction with cached computations.
Accounts for repeated rows in row counting.
"""
function extract_sheet_metadata_optimized(table_node)
    metadata = Dict{String,Any}()
    sizehint!(metadata, 4)  # Pre-size for known keys
    
    # Extract sheet name efficiently
    name_attr = findfirst("@table:name", table_node, ODS_NAMESPACES)
    metadata["name"] = name_attr !== nothing ? nodecontent(name_attr) : "Sheet"
    
    # Find all rows once and expand repeated rows
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)
    expanded_rows = expand_repeated_rows(rows)
    row_count = length(expanded_rows)
    metadata["row_count"] = row_count
    
    if row_count == 0
        metadata["column_count"] = 0
        metadata["estimated_size"] = 0
    else
        # Optimized column counting with early termination for large sheets
        max_cols = compute_max_columns_optimized(expanded_rows)
        metadata["column_count"] = max_cols
        metadata["estimated_size"] = row_count * max_cols
    end
    
    return metadata
end

"""
    compute_max_columns_optimized(expanded_rows)

Efficiently compute maximum column count across rows with sampling for large datasets.
Works with expanded row structure that includes repeated rows.
"""
function compute_max_columns_optimized(expanded_rows)
    n_rows = length(expanded_rows)
    
    # For small sheets, check all rows
    if n_rows <= 100
        max_cols = 0
        @inbounds for row_info in expanded_rows
            cells = findall(".//table:table-cell", row_info.node, ODS_NAMESPACES)
            col_count = sum_cell_repeats_optimized(cells)
            max_cols = max(max_cols, col_count)
        end
        return max_cols
    else
        # For large sheets, use sampling strategy
        return compute_max_columns_sampled(expanded_rows)
    end
end

"""
    compute_max_columns_sampled(expanded_rows)

Use sampling to estimate maximum columns for large datasets.
Works with expanded row structure.
"""
function compute_max_columns_sampled(expanded_rows)
    n_rows = length(expanded_rows)
    # Sample first 50, last 50, and 50 random rows from middle
    sample_size = min(50, n_rows ÷ 3)
    
    max_cols = 0
    
    # First rows
    @inbounds for i in 1:min(sample_size, n_rows)
        cells = findall(".//table:table-cell", expanded_rows[i].node, ODS_NAMESPACES)
        max_cols = max(max_cols, sum_cell_repeats_optimized(cells))
    end
    
    # Last rows
    start_idx = max(n_rows - sample_size + 1, sample_size + 1)
    @inbounds for i in start_idx:n_rows
        cells = findall(".//table:table-cell", expanded_rows[i].node, ODS_NAMESPACES)
        max_cols = max(max_cols, sum_cell_repeats_optimized(cells))
    end
    
    # Middle samples
    if n_rows > 2 * sample_size
        middle_start = sample_size + 1
        middle_end = n_rows - sample_size
        step = max(1, (middle_end - middle_start) ÷ sample_size)
        
        @inbounds for i in middle_start:step:middle_end
            cells = findall(".//table:table-cell", expanded_rows[i].node, ODS_NAMESPACES)
            max_cols = max(max_cols, sum_cell_repeats_optimized(cells))
        end
    end
    
    return max_cols
end

"""
    sum_cell_repeats_optimized(cells)

Efficiently sum cell repeat counts.
"""
@inline function sum_cell_repeats_optimized(cells)
    total = 0
    @inbounds for cell in cells
        total += get_cell_repeat_count_optimized(cell)
    end
    return total
end

"""
    detect_data_structure_optimized(table_node)

Optimized analysis of sheet structure with early termination and caching.
Properly handles repeated rows.
"""
function detect_data_structure_optimized(table_node)
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)
    
    if isempty(rows)
        return (has_headers = false, data_start_row = 1, header_row = nothing)
    end
    
    # Expand repeated rows for accurate structure analysis
    expanded_rows = expand_repeated_rows(rows)
    
    if isempty(expanded_rows)
        return (has_headers = false, data_start_row = 1, header_row = nothing)
    end
    
    # Extract minimal sample for analysis (first 2 rows maximum)
    n_sample = min(2, length(expanded_rows))
    sample_data = Vector{Vector{String}}(undef, n_sample)
    
    @inbounds for i in 1:n_sample
        row_info = expanded_rows[i]
        cells = findall(".//table:table-cell", row_info.node, ODS_NAMESPACES)
        sample_data[i] = [extract_cell_value(cell, ODS_NAMESPACES) for cell in cells]
    end
    
    if isempty(sample_data)
        return (has_headers = false, data_start_row = 1, header_row = nothing)
    end
    
    # Optimized header detection with early returns
    has_headers = detect_headers_heuristic(sample_data)
    
    return (
        has_headers = has_headers,
        data_start_row = has_headers ? 2 : 1,
        header_row = has_headers ? sample_data[1] : nothing
    )
end

"""
    detect_headers_heuristic(sample_data::Vector{Vector{String}})

Fast heuristic-based header detection.
"""
@inline function detect_headers_heuristic(sample_data::Vector{Vector{String}})
    length(sample_data) < 2 && return false
    
    first_row = sample_data[1]
    second_row = sample_data[2]
    
    # Quick empty check
    first_non_empty = count_non_empty_optimized(first_row)
    first_non_empty == 0 && return false
    
    second_non_empty = count_non_empty_optimized(second_row)
    second_non_empty == 0 && return false
    
    # Fast text vs numeric ratio calculation
    first_text_count = count_text_cells_optimized(first_row)
    second_numeric_count = count_numeric_cells_optimized(second_row)
    
    # Heuristic thresholds
    first_text_ratio = first_text_count / first_non_empty
    second_numeric_ratio = second_numeric_count / second_non_empty
    
    return first_text_ratio > 0.5 && second_numeric_ratio > 0.3
end

"""
    count_non_empty_optimized(row::Vector{String})

Fast count of non-empty cells.
"""
@inline function count_non_empty_optimized(row::Vector{String})
    count = 0
    @inbounds for cell in row
        if !isempty(strip(cell))
            count += 1
        end
    end
    return count
end

"""
    count_text_cells_optimized(row::Vector{String})

Fast count of text (non-numeric) cells.
"""
@inline function count_text_cells_optimized(row::Vector{String})
    count = 0
    @inbounds for cell in row
        if !isempty(cell) && !is_parseable_number_optimized(cell)
            count += 1
        end
    end
    return count
end

"""
    count_numeric_cells_optimized(row::Vector{String})

Fast count of numeric cells.
"""
@inline function count_numeric_cells_optimized(row::Vector{String})
    count = 0
    @inbounds for cell in row
        if !isempty(cell) && is_parseable_number_optimized(cell)
            count += 1
        end
    end
    return count
end

# Streaming interface for memory-efficient processing

"""
    extract_table_data_streaming(table_node, callback::Function; header::Bool=true, skipto::Int=1)

Stream table data row-by-row to a callback function for memory-efficient processing.
Properly handles repeated rows.
"""
function extract_table_data_streaming(table_node, callback::Function; header::Bool = true, skipto::Int = 1)
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)
    
    if isempty(rows)
        return
    end
    
    # Expand repeated rows for accurate streaming
    expanded_rows = expand_repeated_rows(rows)
    
    if isempty(expanded_rows) || skipto > length(expanded_rows)
        return
    end
    
    # Handle header row
    if header && skipto <= length(expanded_rows)
        header_info = expanded_rows[skipto]
        header_cells = findall(".//table:table-cell", header_info.node, ODS_NAMESPACES)
        header_data = extract_row_data_optimized(header_cells)
        callback(0, header_data)  # Row 0 indicates header
        start_row = skipto + 1
    else
        start_row = skipto
    end
    
    # Stream data rows
    @inbounds for (idx, row_pos) in enumerate(start_row:length(expanded_rows))
        row_info = expanded_rows[row_pos]
        cells = findall(".//table:table-cell", row_info.node, ODS_NAMESPACES)
        row_data = extract_row_data_optimized(cells)
        callback(idx, row_data)
    end
end

# Legacy compatibility functions

"""
    get_cell_repeat_count(cell_node)

Legacy compatibility wrapper for optimized cell repeat count function.
"""
get_cell_repeat_count(cell_node) = get_cell_repeat_count_optimized(cell_node)

"""
    make_unique_headers(headers::Vector{String})

Legacy compatibility wrapper for optimized header uniqueness function.
"""
make_unique_headers(headers::Vector{String}) = make_unique_headers_optimized(headers)

"""
    extract_sheet_metadata(table_node)

Legacy compatibility wrapper for optimized metadata extraction.
"""
extract_sheet_metadata(table_node) = extract_sheet_metadata_optimized(table_node)

"""
    detect_data_structure(table_node)

Legacy compatibility wrapper for optimized data structure detection.
"""
detect_data_structure(table_node) = detect_data_structure_optimized(table_node)