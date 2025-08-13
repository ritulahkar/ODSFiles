"""
Reading functions for ODSFiles.jl
src/io/reader.jl
"""

"""
    get_sheet_names(filepath_or_ods::Union{String, ODSFile}) -> Vector{String}

Get the names of all sheets in an ODS file.
"""
function get_sheet_names(filepath::String)
    ods = ODSFile(filepath)
    return get_sheet_names(ods)
end

@inline function get_sheet_names(ods::ODSFile)
    return collect(keys(ods.sheets))
end

"""
    read_sheet(filepath_or_ods::Union{String, ODSFile}; kwargs...) -> DataFrame

Read data from a single sheet in an ODS file into a DataFrame.
"""
function read_sheet(
    filepath::String;
    sheet::Union{String,Int,Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    ods = ODSFile(filepath)
    return read_sheet(ods; sheet = sheet, header = header, types = types, skipto = skipto)
end

function read_sheet(
    ods::ODSFile;
    sheet::Union{String,Int,Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    # Select the sheet to read
    table_node = validate_sheet_selection(ods, sheet)

    # Extract data from the table
    df = extract_table_data(table_node; header = header, skipto = skipto)

    # Apply type conversion with optimized dispatch
    df = apply_types_optimized(df, types)

    return df
end

"""
    read_sheets(filepath_or_ods::Union{String, ODSFile}; kwargs...) -> Vector{SheetSpec}

Read multiple sheets from an ODS file and return as SheetSpec objects.
"""
function read_sheets(
    filepath::String;
    sheets::Union{Vector{String},Vector{Int},Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    ods = ODSFile(filepath)
    return read_sheets(
        ods;
        sheets = sheets,
        header = header,
        types = types,
        skipto = skipto,
    )
end

function read_sheets(
    ods::ODSFile;
    sheets::Union{Vector{String},Vector{Int},Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    sheet_names = get_sheet_names(ods)
    
    # Determine which sheets to read with optimized selection
    target_sheets = select_target_sheets(ods, sheets, sheet_names)

    # Pre-allocate vector with known size
    sheet_specs = Vector{SheetSpec}(undef, length(target_sheets))
    
    # Pre-compute actual start row once
    actual_start_row = header ? skipto + 1 : skipto

    # Read each sheet and create SheetSpec objects
    @inbounds for (pos, sheet_name) in enumerate(target_sheets)
        df = read_sheet(
            ods;
            sheet = sheet_name,
            header = header,
            types = types,
            skipto = skipto,
        )

        sheet_specs[pos] = SheetSpec(
            sheet_name,
            df;
            position = pos,
            include_headers = header,
            data_start_row = actual_start_row,
        )
    end

    return sheet_specs
end

"""
    read_all_sheets(filepath_or_ods::Union{String, ODSFile}; kwargs...) -> Dict{String, DataFrame}

Read all sheets from an ODS file and return as a dictionary.
"""
function read_all_sheets(
    filepath_or_ods::Union{String,ODSFile};
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    specs = read_sheets(filepath_or_ods; header = header, types = types, skipto = skipto)
    return create_sheets_dict(specs)
end

# Optimized helper functions

"""
    apply_types_optimized(df::DataFrame, types::Union{Dict,Bool})

Apply type conversion with optimized dispatch.
"""
@inline function apply_types_optimized(df::DataFrame, types::Dict)
    return apply_custom_types(df, types)
end

@inline function apply_types_optimized(df::DataFrame, types::Bool)
    return types ? auto_type_columns(df) : df
end

"""
    select_target_sheets(ods::ODSFile, sheets::Union{Vector{String},Vector{Int},Nothing}, sheet_names::Vector{String})

Optimized sheet selection logic.
"""
@inline function select_target_sheets(ods::ODSFile, sheets::Nothing, sheet_names::Vector{String})
    return sheet_names
end

@inline function select_target_sheets(ods::ODSFile, sheets::Vector{String}, sheet_names::Vector{String})
    return validate_sheet_names(ods, sheets)
end

@inline function select_target_sheets(ods::ODSFile, sheets::Vector{Int}, sheet_names::Vector{String})
    return validate_sheet_indices(ods, sheets)
end

"""
    create_sheets_dict(specs::Vector{SheetSpec})

Create dictionary from SheetSpec vector with optimized allocation.
"""
function create_sheets_dict(specs::Vector{SheetSpec})
    # Pre-allocate dictionary with known size
    result = Dict{String, DataFrame}()
    sizehint!(result, length(specs))
    
    @inbounds for spec in specs
        result[spec.name] = spec.data
    end
    
    return result
end

# Alternative batch reading functions for performance-critical scenarios

"""
    read_sheets_parallel(filepath_or_ods::Union{String, ODSFile}; kwargs...) -> Vector{SheetSpec}

Read multiple sheets in parallel (when beneficial for large files).
Note: Only use when sheets are large and IO-bound operations dominate.
"""
function read_sheets_parallel(
    filepath_or_ods::Union{String,ODSFile};
    sheets::Union{Vector{String},Vector{Int},Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
    min_parallel_sheets::Int = 3,  # Only parallelize if >= 3 sheets
)
    # Open ODS file once
    ods = filepath_or_ods isa String ? ODSFile(filepath_or_ods) : filepath_or_ods
    sheet_names = get_sheet_names(ods)
    target_sheets = select_target_sheets(ods, sheets, sheet_names)
    
    # Use parallel processing only if beneficial
    if length(target_sheets) >= min_parallel_sheets && Threads.nthreads() > 1
        return read_sheets_threaded(ods, target_sheets, header, types, skipto)
    else
        return read_sheets(ods; sheets = target_sheets, header = header, types = types, skipto = skipto)
    end
end

"""
    read_sheets_threaded(ods::ODSFile, target_sheets::Vector{String}, header::Bool, types, skipto::Int)

Internal threaded reading implementation.
"""
function read_sheets_threaded(ods::ODSFile, target_sheets::Vector{String}, header::Bool, types, skipto::Int)
    n_sheets = length(target_sheets)
    sheet_specs = Vector{SheetSpec}(undef, n_sheets)
    actual_start_row = header ? skipto + 1 : skipto
    
    Threads.@threads for i in 1:n_sheets
        @inbounds sheet_name = target_sheets[i]
        
        df = read_sheet(
            ods;
            sheet = sheet_name,
            header = header,
            types = types,
            skipto = skipto,
        )

        @inbounds sheet_specs[i] = SheetSpec(
            sheet_name,
            df;
            position = i,
            include_headers = header,
            data_start_row = actual_start_row,
        )
    end
    
    return sheet_specs
end

"""
    read_sheets_lazy(filepath_or_ods::Union{String, ODSFile}; kwargs...) -> Dict{String, Function}

Return a dictionary of lazy-loaded sheet readers for memory-efficient access.
Each value is a function that returns the DataFrame when called.
"""
function read_sheets_lazy(
    filepath_or_ods::Union{String,ODSFile};
    sheets::Union{Vector{String},Vector{Int},Nothing} = nothing,
    header::Bool = true,
    types::Union{Dict,Bool} = true,
    skipto::Int = 1,
)
    # Store parameters and file reference
    ods = filepath_or_ods isa String ? ODSFile(filepath_or_ods) : filepath_or_ods
    sheet_names = get_sheet_names(ods)
    target_sheets = select_target_sheets(ods, sheets, sheet_names)
    
    # Create lazy loaders
    lazy_dict = Dict{String, Function}()
    sizehint!(lazy_dict, length(target_sheets))
    
    for sheet_name in target_sheets
        # Capture variables in closure
        lazy_dict[sheet_name] = () -> read_sheet(
            ods;
            sheet = sheet_name,
            header = header,
            types = types,
            skipto = skipto,
        )
    end
    
    return lazy_dict
end

"""
    read_sheet_streaming(filepath_or_ods::Union{String, ODSFile}, callback::Function; kwargs...)

Stream sheet data row-by-row to a callback function for memory-efficient processing.
The callback receives (row_index::Int, row_data::Vector) for each row.
"""
function read_sheet_streaming(
    filepath_or_ods::Union{String,ODSFile},
    callback::Function;
    sheet::Union{String,Int,Nothing} = nothing,
    header::Bool = true,
    skipto::Int = 1,
)
    ods = filepath_or_ods isa String ? ODSFile(filepath_or_ods) : filepath_or_ods
    table_node = validate_sheet_selection(ods, sheet)
    
    # Stream processing implementation would go here
    # This is a placeholder for the streaming interface
    extract_table_data_streaming(table_node, callback; header = header, skipto = skipto)
end