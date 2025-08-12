"""
Validation utilities for ODSFiles.jl
src/core/validation.jl
"""

"""
    validate_file_path(filepath::String, overwrite::Bool)

Validate file path for writing operations.
"""
function validate_file_path(filepath::String, overwrite::Bool)
    if isfile(filepath) && !overwrite
        throw(
            ArgumentError(
                "File '$filepath' already exists. Set overwrite=true to replace it.",
            ),
        )
    end

    # Ensure directory exists
    dir = dirname(filepath)
    if !isempty(dir) && !isdir(dir)
        mkpath(dir)
    end

    # Validate file extension
    if !endswith(lowercase(filepath), ".ods")
        @warn "File extension is not '.ods'. The file may not open correctly in spreadsheet applications."
    end
end

"""
    validate_sheet_selection(ods::ODSFile, sheet::Union{String,Int,Nothing})

Validate and return the selected sheet node.
"""
function validate_sheet_selection(ods::ODSFile, sheet::Union{String,Int,Nothing})
    if sheet === nothing
        # Return first sheet
        if isempty(ods.sheets)
            throw(ArgumentError("No sheets found in ODS file"))
        end
        return first(values(ods.sheets))
    elseif sheet isa String
        if !haskey(ods.sheets, sheet)
            available = join(keys(ods.sheets), ", ")
            throw(ArgumentError("Sheet '$sheet' not found. Available sheets: $available"))
        end
        return ods.sheets[sheet]
    elseif sheet isa Int
        sheet_names = collect(keys(ods.sheets))
        if sheet < 1 || sheet > length(sheet_names)
            throw(
                ArgumentError("Sheet index $sheet out of range (1-$(length(sheet_names)))"),
            )
        end
        return ods.sheets[sheet_names[sheet]]
    end
end

"""
    validate_sheet_names(ods::ODSFile, sheets::Vector{String})

Validate that requested sheet names exist in the ODS file.
"""
function validate_sheet_names(ods::ODSFile, sheets::Vector{String})
    sheet_names = collect(keys(ods.sheets))
    missing_sheets = setdiff(sheets, sheet_names)
    if !isempty(missing_sheets)
        available = join(sheet_names, ", ")
        throw(
            ArgumentError(
                "Sheets not found: $(join(missing_sheets, ", ")). Available: $available",
            ),
        )
    end
    return sheets
end

"""
    validate_sheet_indices(ods::ODSFile, sheets::Vector{Int})

Validate and convert sheet indices to names.
"""
function validate_sheet_indices(ods::ODSFile, sheets::Vector{Int})
    sheet_names = collect(keys(ods.sheets))
    if any(i -> i < 1 || i > length(sheet_names), sheets)
        throw(
            ArgumentError(
                "Sheet indices out of range (1-$(length(sheet_names))): $sheets",
            ),
        )
    end
    return [sheet_names[i] for i in sheets]
end

"""
    sort_sheets_by_position(sheets::Vector{SheetSpec})

Sort sheets by position, maintaining original order for unpositioned sheets.
"""
function sort_sheets_by_position(sheets::Vector{SheetSpec})
    positioned = filter(s -> s.position !== nothing, sheets)
    unpositioned = filter(s -> s.position === nothing, sheets)

    # Check for duplicate positions
    if length(positioned) > 0
        positions = [s.position for s in positioned]
        if length(unique(positions)) != length(positions)
            throw(ArgumentError("Duplicate sheet positions found: $(positions)"))
        end
    end

    # Sort positioned sheets and combine with unpositioned ones
    sort!(positioned, by = s -> s.position)
    return vcat(positioned, unpositioned)
end