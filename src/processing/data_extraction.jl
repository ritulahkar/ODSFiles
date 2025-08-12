"""
Data extraction utilities for ODSFiles.jl
src/processing/data_extraction.jl
"""

"""
    extract_table_data(table_node; header::Bool=true, skipto::Int=1)

Extract data from a table XML node into a DataFrame.
"""
function extract_table_data(table_node; header::Bool = true, skipto::Int = 1)
    # Find all rows
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)

    if isempty(rows) || skipto > length(rows)
        return DataFrame()
    end

    # Extract data from all rows starting from skipto
    data_matrix = []
    max_cols = 0

    for row in rows[skipto:end]
        cells = findall(".//table:table-cell", row, ODS_NAMESPACES)
        row_data = String[]

        for cell in cells
            # Handle repeated cells
            repeat_count = get_cell_repeat_count(cell)
            cell_value = extract_cell_value(cell, ODS_NAMESPACES)

            # Add repeated values
            for _ = 1:repeat_count
                push!(row_data, cell_value)
            end
        end

        max_cols = max(max_cols, length(row_data))
        push!(data_matrix, row_data)
    end

    # Pad rows to have the same number of columns
    for row_data in data_matrix
        while length(row_data) < max_cols
            push!(row_data, "")
        end
    end

    # Convert to DataFrame
    if isempty(data_matrix) || max_cols == 0
        return DataFrame()
    end

    # Handle headers
    if header && length(data_matrix) > 0
        headers = [String(strip(cell)) for cell in data_matrix[1]]
        headers = make_unique_headers(headers)
        data_rows = length(data_matrix) > 1 ? data_matrix[2:end] : []
    else
        headers = ["Column$i" for i = 1:max_cols]
        data_rows = data_matrix
    end

    # Convert data matrix to dictionary for DataFrame construction
    data_dict = Dict{String,Vector{String}}()
    for (i, header_name) in enumerate(headers)
        data_dict[header_name] = [length(row) >= i ? row[i] : "" for row in data_rows]
    end

    return DataFrame(data_dict)
end

"""
    get_cell_repeat_count(cell_node)

Get the number of times a cell should be repeated (for merged cells).
"""
function get_cell_repeat_count(cell_node)
    repeat_attr = findfirst("@table:number-columns-repeated", cell_node, ODS_NAMESPACES)
    if repeat_attr !== nothing
        try
            return parse(Int, nodecontent(repeat_attr))
        catch
            return 1
        end
    end
    return 1
end

"""
    make_unique_headers(headers::Vector{String})

Ensure all headers are unique by adding suffixes to duplicates.
"""
function make_unique_headers(headers::Vector{String})
    unique_headers = String[]
    header_counts = Dict{String,Int}()

    for header in headers
        clean_header = isempty(strip(header)) ? "Column" : String(strip(header))

        if haskey(header_counts, clean_header)
            header_counts[clean_header] += 1
            unique_header = "$(clean_header)_$(header_counts[clean_header])"
        else
            header_counts[clean_header] = 0
            unique_header = clean_header
        end

        push!(unique_headers, unique_header)
    end

    return unique_headers
end

"""
    extract_sheet_metadata(table_node)

Extract metadata from a sheet table node.
"""
function extract_sheet_metadata(table_node)
    metadata = Dict{String,Any}()
    
    # Extract sheet name
    name_attr = findfirst("@table:name", table_node, ODS_NAMESPACES)
    metadata["name"] = name_attr !== nothing ? nodecontent(name_attr) : "Sheet"
    
    # Count rows and columns
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)
    metadata["row_count"] = length(rows)
    
    if !isempty(rows)
        # Count maximum columns across all rows
        max_cols = 0
        for row in rows
            cells = findall(".//table:table-cell", row, ODS_NAMESPACES)
            col_count = sum(get_cell_repeat_count(cell) for cell in cells)
            max_cols = max(max_cols, col_count)
        end
        metadata["column_count"] = max_cols
    else
        metadata["column_count"] = 0
    end
    
    return metadata
end

"""
    detect_data_structure(table_node)

Analyze the structure of a sheet to detect headers and data start row.
"""
function detect_data_structure(table_node)
    rows = findall(".//table:table-row", table_node, ODS_NAMESPACES)
    
    if isempty(rows)
        return (has_headers = false, data_start_row = 1, header_row = nothing)
    end
    
    # Extract first few rows for analysis
    sample_rows = []
    for (i, row) in enumerate(rows[1:min(3, length(rows))])
        cells = findall(".//table:table-cell", row, ODS_NAMESPACES)
        row_data = [extract_cell_value(cell, ODS_NAMESPACES) for cell in cells]
        push!(sample_rows, (row_index = i, data = row_data))
    end
    
    if isempty(sample_rows)
        return (has_headers = false, data_start_row = 1, header_row = nothing)
    end
    
    # Simple heuristic: if first row contains mostly text and second row contains numbers/mixed types
    first_row = sample_rows[1].data
    has_headers = false
    
    if length(sample_rows) >= 2
        second_row = sample_rows[2].data
        
        # Count text vs numeric in first row
        first_text_count = count(x -> !isempty(x) && !is_parseable_number(x), first_row)
        first_total_count = count(x -> !isempty(strip(x)), first_row)
        
        # Count numeric in second row
        second_numeric_count = count(x -> !isempty(x) && is_parseable_number(x), second_row)
        second_total_count = count(x -> !isempty(strip(x)), second_row)
        
        # Heuristic: headers if first row is mostly text and second row has some numbers
        if first_total_count > 0 && second_total_count > 0
            first_text_ratio = first_text_count / first_total_count
            second_numeric_ratio = second_numeric_count / second_total_count
            
            has_headers = first_text_ratio > 0.5 && second_numeric_ratio > 0.3
        end
    end
    
    return (
        has_headers = has_headers,
        data_start_row = has_headers ? 2 : 1,
        header_row = has_headers ? first_row : nothing
    )
end