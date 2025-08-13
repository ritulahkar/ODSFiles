"""
Type conversion utilities for ODSFiles.jl
src/processing/type_conversion.jl

Enhanced version with robust type detection similar to CSV.jl
"""

using DataFrames, Dates

# Pre-compiled regular expressions for better performance
const DATE_PATTERNS = [
    r"^\d{4}-\d{1,2}-\d{1,2}$",           # YYYY-MM-DD
    r"^\d{1,2}/\d{1,2}/\d{4}$",           # MM/DD/YYYY or DD/MM/YYYY
    r"^\d{1,2}-\d{1,2}-\d{4}$",           # MM-DD-YYYY or DD-MM-YYYY
    r"^\d{4}/\d{1,2}/\d{1,2}$",           # YYYY/MM/DD
    r"^\d{1,2}\.\d{1,2}\.\d{4}$",         # DD.MM.YYYY
]

# Pre-defined date formats for faster parsing
const DATE_FORMATS = [
    "yyyy-mm-dd",
    "mm/dd/yyyy",
    "dd/mm/yyyy", 
    "yyyy/mm/dd",
    "dd.mm.yyyy",
    "mm-dd-yyyy",
    "dd-mm-yyyy"
]

# Boolean value sets for fast lookup
const TRUE_VALUES = Set(["true", "1", "yes", "y", "t"])
const FALSE_VALUES = Set(["false", "0", "no", "n", "f"])
const BOOL_VALUES = union(TRUE_VALUES, FALSE_VALUES)

# Missing value indicators (similar to CSV.jl)
const MISSING_STRINGS = Set(["", "NA", "N/A", "na", "n/a", "NULL", "null", "missing", "#N/A", "#NULL!"])

"""
    TypeDetectionResult

Holds information about detected column type and whether it allows missing values.
"""
struct TypeDetectionResult
    detected_type::Type
    allows_missing::Bool
    has_missing_values::Bool
end

"""
    auto_type_columns(df::DataFrame; strict_types::Bool=true)

Automatically detect and convert column types in a DataFrame with optimized processing.
If strict_types=true, columns without missing values will have non-Union types (e.g., Int instead of Union{Int, Missing}).
"""
function auto_type_columns(df::DataFrame; strict_types::Bool=true)
    result_df = DataFrame()
    n_rows= nrow(df)
    
    # Pre-allocate result DataFrame columns for better performance
    for col_name in names(df)
        col_data = df[!, col_name]
        
        # Skip empty columns with fast check
        if is_empty_column_fast(col_data)
            result_df[!, col_name] = strict_types ? fill(missing, n_rows) : Vector{Union{String,Missing}}(undef, n_rows)
            continue
        end

        # Convert using optimized type detection
        new_col = convert_column_type_optimized(col_data; strict_types=strict_types)
        result_df[!, col_name] = new_col
    end

    return result_df
end

"""
    is_empty_column_fast(col_data::Vector{String})

Fast check if a column contains only empty values.
"""
@inline function is_empty_column_fast(col_data::Vector{String})
    @inbounds for x in col_data
        stripped = strip(x)
        if !isempty(stripped) && !(stripped in MISSING_STRINGS)
            return false
        end
    end
    return true
end

"""
    analyze_column_values(col_data::Vector{String})

Analyze column data to determine missing values and valid data points.
"""
function analyze_column_values(col_data::Vector{String})
    n_items = length(col_data)
    valid_indices = Int[]
    missing_indices = Int[]
    processed_values = Vector{String}(undef, n_items)
    
    sizehint!(valid_indices, n_items)
    sizehint!(missing_indices, n_items ÷ 10)  # Assume 10% missing
    
    @inbounds for i in 1:n_items
        stripped = strip(col_data[i])
        processed_values[i] = stripped
        
        if isempty(stripped) || (stripped in MISSING_STRINGS)
            push!(missing_indices, i)
        else
            push!(valid_indices, i)
        end
    end
    
    return (
        valid_indices=valid_indices,
        missing_indices=missing_indices,
        processed_values=processed_values,
        has_missing=!isempty(missing_indices)
    )
end

"""
    convert_column_type_optimized(col_data::Vector{String}; strict_types::Bool=true)

Optimized column type conversion with early termination and type-specific paths.
"""
function convert_column_type_optimized(col_data::Vector{String}; strict_types::Bool=true)
    # Analyze column values
    analysis = analyze_column_values(col_data)
    
    if isempty(analysis.valid_indices)
        # All values are missing
        return strict_types && !analysis.has_missing ? String[] : 
               Vector{Union{String,Missing}}([missing for _ in col_data])
    end

    # Detect the best type for non-missing values
    type_result = detect_column_type(analysis.processed_values, analysis.valid_indices)
    
    # Convert based on detected type and missing value policy
    return convert_to_detected_type(
        col_data, 
        analysis.processed_values, 
        analysis.missing_indices,
        type_result, 
        strict_types
    )
end

"""
    detect_column_type(processed_values::Vector{String}, valid_indices::Vector{Int})

Detect the most appropriate type for a column based on valid values.
"""
function detect_column_type(processed_values::Vector{String}, valid_indices::Vector{Int})
    # Try conversions in order of specificity and performance
    
    # Try Bool conversion (fast set-based lookup)
    if try_bool_conversion_fast(processed_values, valid_indices)
        return TypeDetectionResult(Bool, false, false)
    end
    
    # Try Int conversion (fastest numeric type)
    if try_int_conversion_fast(processed_values, valid_indices)
        return TypeDetectionResult(Int, false, false)
    end

    # Try Float conversion
    if try_float_conversion_fast(processed_values, valid_indices)
        return TypeDetectionResult(Float64, false, false)
    end


    # Try Date conversion (most expensive, try last)
    if try_date_conversion_fast(processed_values, valid_indices)
        return TypeDetectionResult(Date, false, false)
    end

    # Default to String
    return TypeDetectionResult(String, false, false)
end

"""
    convert_to_detected_type(col_data, processed_values, missing_indices, type_result, strict_types)

Convert column data to the detected type, handling missing values appropriately.
"""
function convert_to_detected_type(
    col_data::Vector{String}, 
    processed_values::Vector{String}, 
    missing_indices::Vector{Int},
    type_result::TypeDetectionResult, 
    strict_types::Bool
)
    n_items = length(col_data)
    has_missing = !isempty(missing_indices)
    
    # Determine if we need Union with Missing
    needs_union = has_missing && (!strict_types || has_missing)
    use_union = !strict_types || has_missing
    
    if type_result.detected_type == Int
        if use_union
            result = Vector{Union{Int,Missing}}(undef, n_items)
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if isempty(stripped) || (stripped in MISSING_STRINGS)
                    result[i] = missing
                else
                    result[i] = parse(Int, stripped)
                end
            end
        else
            # Pure Int vector - filter out missing values or use default
            valid_values = Int[]
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if !isempty(stripped) && !(stripped in MISSING_STRINGS)
                    push!(valid_values, parse(Int, stripped))
                end
            end
            result = valid_values
        end
        return result
        
    elseif type_result.detected_type == Float64
        if use_union
            result = Vector{Union{Float64,Missing}}(undef, n_items)
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if isempty(stripped) || (stripped in MISSING_STRINGS)
                    result[i] = missing
                else
                    result[i] = parse(Float64, stripped)
                end
            end
        else
            valid_values = Float64[]
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if !isempty(stripped) && !(stripped in MISSING_STRINGS)
                    push!(valid_values, parse(Float64, stripped))
                end
            end
            result = valid_values
        end
        return result
        
    elseif type_result.detected_type == Bool
        if use_union
            result = Vector{Union{Bool,Missing}}(undef, n_items)
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if isempty(stripped) || (stripped in MISSING_STRINGS)
                    result[i] = missing
                else
                    result[i] = parse_bool_flexible_optimized(stripped)
                end
            end
        else
            valid_values = Bool[]
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if !isempty(stripped) && !(stripped in MISSING_STRINGS)
                    push!(valid_values, parse_bool_flexible_optimized(stripped))
                end
            end
            result = valid_values
        end
        return result
        
    elseif type_result.detected_type == Date
        if use_union
            result = Vector{Union{Date,Missing}}(undef, n_items)
            successful_format = ""
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if isempty(stripped) || (stripped in MISSING_STRINGS)
                    result[i] = missing
                else
                    result[i] = parse_date_flexible_optimized(stripped, successful_format)
                end
            end
        else
            valid_values = Date[]
            successful_format = ""
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if !isempty(stripped) && !(stripped in MISSING_STRINGS)
                    push!(valid_values, parse_date_flexible_optimized(stripped, successful_format))
                end
            end
            result = valid_values
        end
        return result
    else
        # String type
        if use_union
            result = Vector{Union{String,Missing}}(undef, n_items)
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if isempty(stripped) || (stripped in MISSING_STRINGS)
                    result[i] = missing
                else
                    result[i] = stripped
                end
            end
        else
            valid_values = String[]
            @inbounds for i in 1:n_items
                stripped = processed_values[i]
                if !isempty(stripped) && !(stripped in MISSING_STRINGS)
                    push!(valid_values, stripped)
                end
            end
            result = valid_values
        end
        return result
    end
end

"""
    try_int_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})

Fast check if all valid values can be converted to integers.
"""
@inline function try_int_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})
    @inbounds for idx in valid_indices
        value = processed_values[idx]
        # Quick character-based pre-check
        if !looks_like_integer_fast(value)
            return false
        end
        # Actual parsing check
        try
            parse(Int, value)
        catch
            return false
        end
    end
    return true
end

"""
    looks_like_integer_fast(s::AbstractString)

Fast heuristic check if string looks like an integer.
"""
@inline function looks_like_integer_fast(s::AbstractString)
    isempty(s) && return false
    
    first_char = s[1]
    if !(isdigit(first_char) || first_char == '-' || first_char == '+')
        return false
    end
    
    # Check for decimal point (disqualifies as integer)
    return !('.' in s || 'e' in s || 'E' in s)
end

"""
    try_float_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})

Fast check if all valid values can be converted to floats.
"""
@inline function try_float_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})
    @inbounds for idx in valid_indices
        value = processed_values[idx]
        if !is_parseable_number_optimized(value)
            return false
        end
    end
    return true
end

# """
#     is_parseable_number_optimized(s::AbstractString)

# Optimized check if a string can be parsed as a number.
# """
# @inline function is_parseable_number_optimized(s::AbstractString)
#     isempty(s) && return false
    
#     # Quick heuristic checks
#     first_char = s[1]
#     if !(isdigit(first_char) || first_char in ['-', '+', '.'])
#         return false
#     end
    
#     # Check for scientific notation or special float values
#     if occursin(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$", s) ||
#        lowercase(s) in ["inf", "-inf", "+inf", "infinity", "-infinity", "+infinity", "nan"]
#         return true
#     end
    
#     # Fallback to actual parsing
#     try
#         parse(Float64, s)
#         return true
#     catch
#         return false
#     end
# end

"""
    try_bool_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})

Fast check if all valid values can be converted to booleans using set lookup.
"""
function try_bool_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})
    @inbounds for idx in valid_indices
        value = lowercase(processed_values[idx])
        if !(value in BOOL_VALUES)
            return false
        end
    end
    return true
end

"""
    try_date_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})

Fast check if all valid values look like dates using pre-compiled regex.
"""
function try_date_conversion_fast(processed_values::Vector{String}, valid_indices::Vector{Int})
    @inbounds for idx in valid_indices
        value = processed_values[idx]
        if !looks_like_date_optimized(value)
            return false
        end
    end
    return true
end

"""
    apply_custom_types_optimized(df::DataFrame, type_dict::Dict; strict_types::Bool=true)

Optimized custom type conversion with batch processing.
"""
function apply_custom_types_optimized(df::DataFrame, type_dict::Dict; strict_types::Bool=true)
    result_df = DataFrame()
    
    # Process columns efficiently
    for col_name in names(df)
        if haskey(type_dict, col_name)
            target_type = type_dict[col_name]
            col_data = df[!, col_name]
            try
                new_col = convert_to_type_optimized(col_data, target_type; strict_types=strict_types)
                result_df[!, col_name] = new_col
            catch e
                @warn "Failed to convert column $col_name to $target_type: $e"
                result_df[!, col_name] = col_data  # Keep original on failure
            end
        else
            result_df[!, col_name] = df[!, col_name]  # Copy unchanged
        end
    end

    return result_df
end

"""
    convert_to_type_optimized(col_data::Vector{String}, target_type::Type; strict_types::Bool=true)

Optimized type conversion with specialized methods for each type.
"""
function convert_to_type_optimized(col_data::Vector{String}, target_type::Type; strict_types::Bool=true)
    analysis = analyze_column_values(col_data)
    use_union = !strict_types || analysis.has_missing
    
    if target_type == Int
        return convert_to_int_with_policy(col_data, analysis, use_union)
    elseif target_type == Float64
        return convert_to_float_with_policy(col_data, analysis, use_union)
    elseif target_type == Bool
        return convert_to_bool_with_policy(col_data, analysis, use_union)
    elseif target_type == String
        return convert_to_string_with_policy(col_data, analysis, use_union)
    elseif target_type == Date
        return convert_to_date_with_policy(col_data, analysis, use_union)
    else
        throw(ArgumentError("Unsupported type: $target_type"))
    end
end

# Helper functions for type conversion with policy
function convert_to_int_with_policy(col_data::Vector{String}, analysis, use_union::Bool)
    n_items = length(col_data)
    if use_union
        result = Vector{Union{Int,Missing}}(undef, n_items)
        @inbounds for i in 1:n_items
            stripped = analysis.processed_values[i]
            if i in analysis.missing_indices
                result[i] = missing
            else
                result[i] = parse(Int, stripped)
            end
        end
        return result
    else
        # Return only valid values
        valid_values = Int[]
        @inbounds for idx in analysis.valid_indices
            push!(valid_values, parse(Int, analysis.processed_values[idx]))
        end
        return valid_values
    end
end

function convert_to_float_with_policy(col_data::Vector{String}, analysis, use_union::Bool)
    n_items = length(col_data)
    if use_union
        result = Vector{Union{Float64,Missing}}(undef, n_items)
        @inbounds for i in 1:n_items
            stripped = analysis.processed_values[i]
            if i in analysis.missing_indices
                result[i] = missing
            else
                result[i] = parse(Float64, stripped)
            end
        end
        return result
    else
        valid_values = Float64[]
        @inbounds for idx in analysis.valid_indices
            push!(valid_values, parse(Float64, analysis.processed_values[idx]))
        end
        return valid_values
    end
end

function convert_to_bool_with_policy(col_data::Vector{String}, analysis, use_union::Bool)
    n_items = length(col_data)
    if use_union
        result = Vector{Union{Bool,Missing}}(undef, n_items)
        @inbounds for i in 1:n_items
            stripped = analysis.processed_values[i]
            if i in analysis.missing_indices
                result[i] = missing
            else
                result[i] = parse_bool_flexible_optimized(stripped)
            end
        end
        return result
    else
        valid_values = Bool[]
        @inbounds for idx in analysis.valid_indices
            push!(valid_values, parse_bool_flexible_optimized(analysis.processed_values[idx]))
        end
        return valid_values
    end
end

function convert_to_string_with_policy(col_data::Vector{String}, analysis, use_union::Bool)
    n_items = length(col_data)
    if use_union
        result = Vector{Union{String,Missing}}(undef, n_items)
        @inbounds for i in 1:n_items
            if i in analysis.missing_indices
                result[i] = missing
            else
                result[i] = analysis.processed_values[i]
            end
        end
        return result
    else
        valid_values = String[]
        @inbounds for idx in analysis.valid_indices
            push!(valid_values, analysis.processed_values[idx])
        end
        return valid_values
    end
end

function convert_to_date_with_policy(col_data::Vector{String}, analysis, use_union::Bool)
    n_items = length(col_data)
    successful_format = ""
    
    if use_union
        result = Vector{Union{Date,Missing}}(undef, n_items)
        @inbounds for i in 1:n_items
            if i in analysis.missing_indices
                result[i] = missing
            else
                result[i] = parse_date_flexible_optimized(analysis.processed_values[i], successful_format)
            end
        end
        return result
    else
        valid_values = Date[]
        @inbounds for idx in analysis.valid_indices
            push!(valid_values, parse_date_flexible_optimized(analysis.processed_values[idx], successful_format))
        end
        return valid_values
    end
end

"""
    parse_bool_flexible_optimized(s::AbstractString)

Optimized boolean parsing using pre-computed sets.
"""
@inline function parse_bool_flexible_optimized(s::AbstractString)
    lower_s = lowercase(strip(s))
    if lower_s in TRUE_VALUES
        return true
    elseif lower_s in FALSE_VALUES
        return false
    else
        throw(ArgumentError("Cannot parse '$s' as boolean"))
    end
end

"""
    looks_like_date_optimized(s::AbstractString)

Optimized date pattern matching using pre-compiled regex patterns.
"""
@inline function looks_like_date_optimized(s::AbstractString)
    isempty(s) && return false
    
    @inbounds for pattern in DATE_PATTERNS
        if occursin(pattern, s)
            return true
        end
    end
    return false
end

"""
    parse_date_flexible_optimized(s::AbstractString, successful_format::String)

Optimized date parsing with format caching and early termination.
"""
function parse_date_flexible_optimized(s::AbstractString, successful_format::String)
    s = strip(s)
    
    # Try the previously successful format first
    if !isempty(successful_format)
        try
            return Date(s, successful_format)
        catch
            # Continue to try other formats
        end
    end
    
    # Try pre-defined formats
    @inbounds for format in DATE_FORMATS
        format == successful_format && continue  # Skip already tried
        try
            result = Date(s, format)
            successful_format = format  # Cache for next time
            return result
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

# Batch conversion utilities for better performance

"""
    convert_columns_batch(df::DataFrame, type_specs::Vector{Pair{String, Type}}; strict_types::Bool=true)

Convert multiple columns in a single pass for better performance.
"""
function convert_columns_batch(df::DataFrame, type_specs::Vector{Pair{String, Type}}; strict_types::Bool=true)
    result_df = copy(df)
    
    @inbounds for (col_name, target_type) in type_specs
        if col_name in names(df)
            col_data = df[!, col_name]
            try
                new_col = convert_to_type_optimized(col_data, target_type; strict_types=strict_types)
                result_df[!, col_name] = new_col
            catch e
                @warn "Failed to convert column $col_name to $target_type: $e"
            end
        end
    end
    
    return result_df
end

"""
    auto_type_columns_parallel(df::DataFrame; min_cols_for_parallel::Int = 10, strict_types::Bool=true)

Parallel version of auto_type_columns for large DataFrames.
"""
function auto_type_columns_parallel(df::DataFrame; min_cols_for_parallel::Int = 10, strict_types::Bool=true)
    col_names = names(df)
    n_cols = length(col_names)
    
    # Only use parallel processing if beneficial
    if n_cols < min_cols_for_parallel || Threads.nthreads() == 1
        return auto_type_columns(df; strict_types=strict_types)
    end
    
    # Process columns in parallel
    results = Vector{Any}(undef, n_cols)
    
    Threads.@threads for i in 1:n_cols
        col_name = col_names[i]
        col_data = df[!, col_name]
        
        if is_empty_column_fast(col_data)
            results[i] = strict_types ? String[] : Vector{Union{String,Missing}}(undef, 0)
        else
            results[i] = convert_column_type_optimized(col_data; strict_types=strict_types)
        end
    end
    
    # Construct result DataFrame
    result_df = DataFrame()
    @inbounds for i in 1:n_cols
        result_df[!, col_names[i]] = results[i]
    end
    
    return result_df
end

# =============================================================================
# LEGACY COMPATIBILITY API (preserves old function signatures)
# =============================================================================

"""
    convert_column_type(col_data::Vector{String})

Legacy compatibility wrapper for optimized column type conversion.
Maintains backward compatibility with relaxed typing (allows Union types).
"""
convert_column_type(col_data::Vector{String}) = convert_column_type_optimized(col_data; strict_types=false)

"""
    apply_custom_types(df::DataFrame, type_dict::Dict)

Legacy compatibility wrapper for optimized custom type application.
Maintains backward compatibility with relaxed typing (allows Union types).
"""
apply_custom_types(df::DataFrame, type_dict::Dict) = apply_custom_types_optimized(df, type_dict; strict_types=false)

"""
    convert_to_type(col_data::Vector{String}, target_type::Type)

Legacy compatibility wrapper for optimized type conversion.
Maintains backward compatibility with relaxed typing (allows Union types).
"""
convert_to_type(col_data::Vector{String}, target_type::Type) = convert_to_type_optimized(col_data, target_type; strict_types=false)

"""
    parse_bool_flexible(s::AbstractString)

Legacy compatibility wrapper for optimized boolean parsing.
"""
parse_bool_flexible(s::AbstractString) = parse_bool_flexible_optimized(s)

"""
    looks_like_date(s::AbstractString)

Legacy compatibility wrapper for optimized date pattern matching.
"""
looks_like_date(s::AbstractString) = looks_like_date_optimized(s)

"""
    parse_date_flexible(s::AbstractString)

Legacy compatibility wrapper for optimized date parsing.
"""
parse_date_flexible(s::AbstractString) = parse_date_flexible_optimized(s, "")

# =============================================================================
# CSV.jl-like Interface
# =============================================================================

"""
    detect_types(df::DataFrame; 
                strict::Bool=true, 
                missingstrings::Vector{String}=["", "NA", "NULL"],
                truestrings::Vector{String}=["true", "1", "yes"],
                falsestrings::Vector{String}=["false", "0", "no"])

CSV.jl-like type detection interface.
- strict: if true, columns without missing values get non-Union types
- missingstrings: additional strings to treat as missing
- truestrings: additional strings to treat as true
- falsestrings: additional strings to treat as false
"""
function detect_types(df::DataFrame; 
                     strict::Bool=true, 
                     missingstrings::Vector{String}=String[],
                     truestrings::Vector{String}=String[],
                     falsestrings::Vector{String}=String[])
    
    # Temporarily modify global constants if custom values provided
    local old_missing = MISSING_STRINGS
    local old_true = TRUE_VALUES  
    local old_false = FALSE_VALUES
    
    if !isempty(missingstrings)
        empty!(MISSING_STRINGS)
        union!(MISSING_STRINGS, Set(missingstrings))
    end
    if !isempty(truestrings)
        empty!(TRUE_VALUES)
        union!(TRUE_VALUES, Set(lowercase.(truestrings)))
    end
    if !isempty(falsestrings)
        empty!(FALSE_VALUES)
        union!(FALSE_VALUES, Set(lowercase.(falsestrings)))
    end
    
    try
        return auto_type_columns(df; strict_types=strict)
    finally
        # Restore original constants
        if !isempty(missingstrings)
            empty!(MISSING_STRINGS)
            union!(MISSING_STRINGS, old_missing)
        end
        if !isempty(truestrings)
            empty!(TRUE_VALUES)
            union!(TRUE_VALUES, old_true)
        end
        if !isempty(falsestrings)
            empty!(FALSE_VALUES)
            union!(FALSE_VALUES, old_false)
        end
    end
end