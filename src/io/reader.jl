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

function get_sheet_names(ods::ODSFile)
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

    # Apply type conversion
    if types isa Dict
        df = apply_custom_types(df, types)
    elseif types === true
        df = auto_type_columns(df)
    end
    # If types === false, keep all columns as strings

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

    # Determine which sheets to read
    if sheets === nothing
        target_sheets = sheet_names
    elseif sheets isa Vector{String}
        target_sheets = validate_sheet_names(ods, sheets)
    elseif sheets isa Vector{Int}
        target_sheets = validate_sheet_indices(ods, sheets)
    end

    # Read each sheet and create SheetSpec objects
    sheet_specs = SheetSpec[]

    for (pos, sheet_name) in enumerate(target_sheets)
        df = read_sheet(
            ods;
            sheet = sheet_name,
            header = header,
            types = types,
            skipto = skipto,
        )

        # Determine if headers were included based on reading parameters
        actual_start_row = header ? skipto + 1 : skipto

        spec = SheetSpec(
            sheet_name,
            df;
            position = pos,
            include_headers = header,
            data_start_row = actual_start_row,
        )
        push!(sheet_specs, spec)
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
    return Dict(spec.name => spec.data for spec in specs)
end