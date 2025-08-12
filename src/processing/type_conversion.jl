"""
Type conversion utilities for ODSFiles.jl
src/processing/type_conversion.jl
"""

"""
    auto_type_columns(df::DataFrame)

Automatically detect and convert column types in a DataFrame.
"""
function auto_type_columns(df::DataFrame)
    result_df = copy(df)

    for col_name in names(df)
        col_data = df[!, col_name]

        # Skip empty columns
        if all(isempty ∘ strip, col_data)
            continue
        end

        # Try to convert to appropriate types
        new_col = convert_column_type(col_data)
        result_df[!, col_name] = new_col
    end

    return result_df
end

"""
    convert_column_type(col_data::Vector{String})

Convert a column of strings to the most appropriate type.
"""
function convert_column_type(col_data::Vector{String})
    non_empty_data = [strip(x) for x in col_data if !isempty(strip(x))]

    if isempty(non_empty_data)
        return col_data
    end

    # Try Int conversion
    try
        int_data = [isempty(strip(x)) ? missing : parse(Int, strip(x)) for x in col_data]
        return int_data
    catch
    end

    # Try Float conversion
    try
        float_data =
            [isempty(strip(x)) ? missing : parse(Float64, strip(x)) for x in col_data]
        return float_data
    catch
    end

    # Try Bool conversion
    if all(
        lowercase(strip(x)) in ["true", "false", "1", "0", "yes", "no", ""] for
        x in col_data
    )
        try
            bool_data = Vector{Union{Bool,Missing}}(undef, length(col_data))
            for (i, x) in enumerate(col_data)
                stripped = lowercase(strip(x))
                if isempty(stripped)
                    bool_data[i] = missing
                elseif stripped in ["true", "1", "yes"]
                    bool_data[i] = true
                elseif stripped in ["false", "0", "no"]
                    bool_data[i] = false
                end
            end
            return bool_data
        catch
        end
    end

    # Try Date conversion
    if all(looks_like_date(strip(x)) || isempty(strip(x)) for x in col_data)
        try
            date_data = Vector{Union{Date,Missing}}(undef, length(col_data))
            for (i, x) in enumerate(col_data)
                stripped = strip(x)
                if isempty(stripped)
                    date_data[i] = missing
                else
                    date_data[i] = parse_date_flexible(stripped)
                end
            end
            return date_data
        catch
        end
    end

    # Return as strings if no conversion worked
    return [isempty(strip(x)) ? missing : strip(x) for x in col_data]
end

"""
    apply_custom_types(df::DataFrame, type_dict::Dict)

Apply custom type conversions to specific columns.
"""
function apply_custom_types(df::DataFrame, type_dict::Dict)
    result_df = copy(df)

    for (col_name, target_type) in type_dict
        if col_name in names(df)
            col_data = df[!, col_name]
            try
                new_col = convert_to_type(col_data, target_type)
                result_df[!, col_name] = new_col
            catch e
                @warn "Failed to convert column $col_name to $target_type: $e"
            end
        end
    end

    return result_df
end

"""
    convert_to_type(col_data::Vector{String}, target_type::Type)

Convert a column to a specific type.
"""
function convert_to_type(col_data::Vector{String}, target_type::Type)
    if target_type == Int
        return [isempty(strip(x)) ? missing : parse(Int, strip(x)) for x in col_data]
    elseif target_type == Float64
        return [isempty(strip(x)) ? missing : parse(Float64, strip(x)) for x in col_data]
    elseif target_type == Bool
        return [isempty(strip(x)) ? missing : parse_bool_flexible(strip(x)) for x in col_data]
    elseif target_type == String
        return [isempty(strip(x)) ? missing : strip(x) for x in col_data]
    elseif target_type == Date
        return [isempty(strip(x)) ? missing : parse_date_flexible(strip(x)) for x in col_data]
    else
        throw(ArgumentError("Unsupported type: $target_type"))
    end
end

"""
    parse_bool_flexible(s::AbstractString)

Parse a string as boolean with flexible input formats.
"""
function parse_bool_flexible(s::AbstractString)
    lower_s = lowercase(strip(s))
    if lower_s in ["true", "1", "yes", "y", "t"]
        return true
    elseif lower_s in ["false", "0", "no", "n", "f"]
        return false
    else
        throw(ArgumentError("Cannot parse '$s' as boolean"))
    end
end

"""
    looks_like_date(s::AbstractString)

Check if a string looks like it could be a date.
"""
function looks_like_date(s::AbstractString)
    if isempty(s)
        return false
    end
    
    # Common date patterns
    date_patterns = [
        r"^\d{4}-\d{1,2}-\d{1,2}$",           # YYYY-MM-DD
        r"^\d{1,2}/\d{1,2}/\d{4}$",           # MM/DD/YYYY or DD/MM/YYYY
        r"^\d{1,2}-\d{1,2}-\d{4}$",           # MM-DD-YYYY or DD-MM-YYYY
        r"^\d{4}/\d{1,2}/\d{1,2}$",           # YYYY/MM/DD
        r"^\d{1,2}\.\d{1,2}\.\d{4}$",         # DD.MM.YYYY
    ]
    
    return any(occursin(pattern, s) for pattern in date_patterns)
end

"""
    parse_date_flexible(s::AbstractString)

Parse a date string with multiple format support.
"""
function parse_date_flexible(s::AbstractString)
    s = strip(s)
    
    # Try common date formats
    date_formats = [
        "yyyy-mm-dd",
        "mm/dd/yyyy",
        "dd/mm/yyyy", 
        "yyyy/mm/dd",
        "dd.mm.yyyy",
        "mm-dd-yyyy",
        "dd-mm-yyyy"
    ]
    
    for format in date_formats
        try
            return Date(s, format)
        catch
            continue
        end
    end
    
    # If no format worked, try Julia's default parsing
    try
        return Date(s)
    catch
        throw(ArgumentError("Cannot parse '$s' as date"))
    end
end