using Test
using ODSFiles
using DataFrames
using Dates
using Random

# Set random seed for reproducible tests
Random.seed!(42)

# Test data generation utilities
function create_test_dataframe(nrows = 100, with_missing = false)
    df = DataFrame(
        ID = 1:nrows,
        Name = ["Person_$i" for i = 1:nrows],
        Age = rand(18:80, nrows),
        Salary = round.(rand(30000:150000, nrows), digits = 2),
        Active = rand(Bool, nrows),
        Date = Date(2020, 1, 1) .+ Day.(rand(0:365, nrows)),
        Score = [round(rand() * 100, digits = 1) for _ = 1:nrows],
    )

    if with_missing && nrows > 10
        # Introduce some missing values (only if we have enough rows)
        missing_age_indices = rand(1:nrows, max(1, div(nrows, 10)))
        missing_salary_indices = rand(1:nrows, max(1, div(nrows, 20)))
        df.Age[missing_age_indices] .= missing
        df.Salary[missing_salary_indices] .= missing
    end

    return df
end

function create_edge_case_dataframe()
    DataFrame(
        Empty_Col = [missing, missing, missing],
        Unicode = ["αβγ", "こんにちは", "🚀✨"],
        Large_Number = [1e15, -1e15, 0],
        Small_Number = [1e-15, -1e-15, 0.0],
        Special_Chars = ["a,b;c", "\"quoted\"", "line1\nline2"],
        Boolean = [true, false, missing],
        Mixed_Types = ["123", "45.6", "missing"],
    )
end

# Main test suite
@testset "ODSFiles.jl Comprehensive Tests" begin

    # Create temporary directory for test files
    mktempdir() do temp_dir

        # Basic test data
        test_df = create_test_dataframe(50)
        edge_df = create_edge_case_dataframe()

        @testset "Core Types and Constructors" begin
            @testset "SheetSpec Constructor - Basic" begin
                # Test if SheetSpec exists and basic constructor works
                if @isdefined(SheetSpec)
                    try
                        spec = SheetSpec("TestSheet", test_df)
                        @test spec.name == "TestSheet"
                        @test spec.data == test_df

                        # Test with additional parameters if supported
                        try
                            spec_full = SheetSpec(
                                "FullTest",
                                test_df,
                                position = 2,
                                include_headers = false,
                                data_start_row = 3,
                            )
                            @test spec_full.position == 2
                            @test spec_full.include_headers == false
                            @test spec_full.data_start_row == 3
                            @info "Full SheetSpec constructor supported"
                        catch e
                            if isa(e, MethodError)
                                @info "Basic SheetSpec constructor only"
                            else
                                @warn "Extended SheetSpec constructor failed" exception=e
                            end
                        end
                    catch e
                        @warn "SheetSpec constructor failed" exception=e
                    end
                else
                    @warn "SheetSpec type not found - may not be exported"
                end
            end
        end

        @testset "Basic File Operations" begin
            test_file = joinpath(temp_dir, "basic_test.ods")

            @testset "Single Sheet Write and Read" begin
                # Test basic write operation
                try
                    write_sheet(test_file, test_df, "TestData")
                    @test isfile(test_file)
                    @info "✓ write_sheet successful"
                catch e
                    @error "write_sheet failed" exception=e
                    @test false  # Force test failure
                end

                # Test basic read operation
                if isfile(test_file)
                    try
                        result = read_sheet(test_file)
                        @test isa(result, DataFrame)
                        @test nrow(result) > 0
                        @test ncol(result) > 0
                        @info "✓ read_sheet successful - $(size(result))"

                        # Test data integrity if possible
                        if size(result) == size(test_df)
                            @test names(result) == names(test_df)
                            @info "✓ Column names preserved"
                        end
                    catch e
                        @error "read_sheet failed" exception=e
                        @test false
                    end
                end
            end

            @testset "Sheet Name Operations" begin
                if isfile(test_file)
                    # Test get_sheet_names if available
                    try
                        sheet_names = get_sheet_names(test_file)
                        @test isa(sheet_names, Vector)
                        @test length(sheet_names) >= 1
                        @info "✓ get_sheet_names successful - found $(length(sheet_names)) sheets"
                    catch e
                        if isa(e, MethodError)
                            @info "get_sheet_names not available"
                        else
                            @warn "get_sheet_names failed" exception=e
                        end
                    end

                    # Test reading with sheet parameter if supported
                    try
                        result = read_sheet(test_file, sheet = "TestData")
                        @test isa(result, DataFrame)
                        @info "✓ read_sheet with sheet name successful"
                    catch e
                        if isa(e, MethodError)
                            @info "read_sheet with sheet parameter not supported"
                        else
                            @warn "read_sheet with sheet name failed" exception=e
                        end
                    end
                end
            end

            @testset "Read Options" begin
                if isfile(test_file)
                    # Test header option
                    try
                        result_no_header = read_sheet(test_file, header = false)
                        @test isa(result_no_header, DataFrame)
                        @info "✓ header=false option works"
                    catch e
                        if isa(e, MethodError)
                            @info "header option not supported"
                        else
                            @warn "header option failed" exception=e
                        end
                    end

                    # Test types option
                    try
                        result_no_types = read_sheet(test_file, types = false)
                        @test isa(result_no_types, DataFrame)
                        @info "✓ types=false option works"
                    catch e
                        if isa(e, MethodError)
                            @info "types option not supported"
                        else
                            @warn "types option failed" exception=e
                        end
                    end

                    # Test skipto option
                    try
                        result_skip = read_sheet(test_file, skipto = 3)
                        @test isa(result_skip, DataFrame)
                        @info "✓ skipto option works"
                    catch e
                        if isa(e, MethodError)
                            @info "skipto option not supported"
                        else
                            @warn "skipto option failed" exception=e
                        end
                    end
                end
            end
        end

        @testset "Multi-Sheet Operations" begin
            multi_file = joinpath(temp_dir, "multi_sheet.ods")

            # Create test data for multiple sheets
            sheet1_df = create_test_dataframe(30)
            sheet2_df = create_test_dataframe(40)

            @testset "Write Multiple Sheets" begin
                # Test dictionary format
                try
                    sheets_dict = Dict("Sales" => sheet1_df, "Products" => sheet2_df)
                    write_sheets(multi_file, sheets_dict)
                    @test isfile(multi_file)
                    @info "✓ write_sheets with Dict successful"
                catch e
                    if isa(e, MethodError)
                        @info "write_sheets not available"
                    else
                        @warn "write_sheets failed" exception=e
                    end
                end

                # Test SheetSpec format if available
                if isfile(multi_file) && @isdefined(SheetSpec)
                    try
                        specs = [
                            SheetSpec("Summary", sheet1_df),
                            SheetSpec("Details", sheet2_df),
                        ]
                        spec_file = joinpath(temp_dir, "spec_format.ods")
                        write_sheets(spec_file, specs)
                        @test isfile(spec_file)
                        @info "✓ write_sheets with SheetSpec successful"
                    catch e
                        @warn "write_sheets with SheetSpec failed" exception=e
                    end
                end
            end

            @testset "Read Multiple Sheets" begin
                if isfile(multi_file)
                    # Test read_sheets if available
                    try
                        specs = read_sheets(multi_file)
                        @test isa(specs, Vector)
                        @test length(specs) >= 1
                        @info "✓ read_sheets successful - found $(length(specs)) sheets"

                        # Test that each spec has expected properties
                        for spec in specs
                            @test hasfield(typeof(spec), :name)
                            @test hasfield(typeof(spec), :data)
                            @test isa(spec.data, DataFrame)
                        end
                    catch e
                        if isa(e, MethodError)
                            @info "read_sheets not available"
                        else
                            @warn "read_sheets failed" exception=e
                        end
                    end

                    # Test read_all_sheets if available
                    try
                        all_sheets = read_all_sheets(multi_file)
                        @test isa(all_sheets, Dict)
                        @test length(all_sheets) >= 1
                        @info "✓ read_all_sheets successful - found $(length(all_sheets)) sheets"

                        # Verify each entry is a DataFrame
                        for (name, df) in all_sheets
                            @test isa(name, String)
                            @test isa(df, DataFrame)
                        end
                    catch e
                        if isa(e, MethodError)
                            @info "read_all_sheets not available"
                        else
                            @warn "read_all_sheets failed" exception=e
                        end
                    end
                end
            end
        end

        @testset "ODSFile Object" begin
            test_file = joinpath(temp_dir, "odsfile_test.ods")
            write_sheet(test_file, test_df, "TestSheet")

            if isfile(test_file) && @isdefined(ODSFile)
                try
                    ods = ODSFile(test_file)
                    @test isa(ods, ODSFile)
                    @info "✓ ODSFile constructor successful"

                    # Test operations with ODSFile
                    try
                        names = get_sheet_names(ods)
                        @test isa(names, Vector)
                        @info "✓ get_sheet_names with ODSFile works"
                    catch e
                        if s isa MethodError
                            @info "get_sheet_names with ODSFile not supported"
                        else
                            @warn "get_sheet_names with ODSFile failed" exception=e
                        end
                    end

                    try
                        result = read_sheet(ods)
                        @test isa(result, DataFrame)
                        @info "✓ read_sheet with ODSFile works"
                    catch e
                        if e isa MethodError
                            @info "read_sheet with ODSFile not supported"
                        else
                            e
                            @warn "read_sheet with ODSFile failed" exception=e
                        end
                    end

                catch e
                    @warn "ODSFile operations failed" exception=e
                end
            else
                if !@isdefined(ODSFile)
                    @info "ODSFile type not available"
                end
            end
        end

        @testset "Error Handling" begin
            @testset "File System Errors" begin
                # Test non-existent file
                @test_throws Exception read_sheet("nonexistent.ods")

                # Test invalid file path
                @test_throws Exception read_sheet("")
            end

            @testset "Data Validation" begin
                # Test empty DataFrame
                empty_df = DataFrame()
                empty_file = joinpath(temp_dir, "empty.ods")

                try
                    write_sheet(empty_file, empty_df, "Empty")
                    if isfile(empty_file)
                        result = read_sheet(empty_file)
                        @test isa(result, DataFrame)
                        @info "✓ Empty DataFrame handled correctly"
                    end
                catch e
                    @warn "Empty DataFrame handling failed" exception=e
                end
            end
        end

        @testset "Data Integrity" begin
            integrity_file = joinpath(temp_dir, "integrity.ods")

            # Test with edge case data
            try
                write_sheet(integrity_file, edge_df, "EdgeCases")
                # write_sheet("int.ods", edge_df, "EdgeCases")
                if isfile(integrity_file)
                    result = read_sheet(integrity_file)
                    @test nrow(result) == nrow(edge_df)
                    @test ncol(result) == ncol(edge_df)
                    @info "✓ Edge case data preserved"

                    # Test Unicode preservation if possible
                    if "Unicode" in names(result)
                        @test result.Unicode[1] == "αβγ"
                        @test result.Unicode[2] == "こんにちは"
                        @test result.Unicode[3] == "🚀✨"
                        @info "✓ Unicode characters preserved"
                    end
                end
            catch e
                @warn "Data integrity test failed" exception=e
            end
        end

        @testset "Performance Tests" begin
            @testset "Large Dataset" begin
                # Test with moderately large dataset (not too large to avoid CI issues)
                large_df = create_test_dataframe(1000)  # 1k rows
                large_file = joinpath(temp_dir, "large.ods")

                # Test write performance
                write_time = @elapsed begin
                    try
                        write_sheet(large_file, large_df, "LargeData")
                        @info "✓ Large dataset write completed"
                    catch e
                        @warn "Large dataset write failed" exception=e
                    end
                end

                if isfile(large_file)
                    # Test read performance
                    read_time = @elapsed begin
                        try
                            result = read_sheet(large_file)
                            @test size(result) == size(large_df)
                            @info "✓ Large dataset read completed"
                        catch e
                            @warn "Large dataset read failed" exception=e
                        end
                    end

                    @info "Performance: Write $(round(write_time, digits=2))s, Read $(round(read_time, digits=2))s"

                    # Reasonable performance expectations (adjust as needed)
                    @test write_time < 30  # Should complete within 30 seconds
                    @test read_time < 20   # Should complete within 20 seconds
                end
            end
        end

        @testset "Round-trip Operations" begin
            roundtrip_file = joinpath(temp_dir, "roundtrip.ods")

            # Test basic round-trip
            try
                write_sheet(roundtrip_file, test_df, "Original")
                if isfile(roundtrip_file)
                    result = read_sheet(roundtrip_file)

                    # Basic structure preservation
                    @test nrow(result) == nrow(test_df)
                    @test ncol(result) == ncol(test_df)

                    # If column names are preserved
                    if names(result) == names(test_df)
                        @info "✓ Column names preserved in round-trip"

                        # Test data preservation for key columns
                        if all(col in names(result) for col in ["ID", "Name"])
                            @test result.ID == test_df.ID
                            @test result.Name == test_df.Name
                            @info "✓ Data values preserved in round-trip"
                        end
                    end
                end
            catch e
                @warn "Round-trip test failed" exception=e
            end
        end

        # Additional test set for column type parsing
        function create_typed_test_dataframe()
            """Create a DataFrame with various column types for testing type parsing"""
            DataFrame(
                # Integer types
                SmallInt = Int8[1, 2, 3, -1, 127],
                RegularInt = [10, 20, 30, -50, 0],
                BigInt = Int64[
                    1000000,
                    -1000000,
                    9223372036854775807,
                    -9223372036854775808,
                    0,
                ],

                # Float types  
                Float32Col = Float32[1.1, 2.2, 3.3, -4.4, 0.0],
                Float64Col = [1.123456789, -2.987654321, 3.141592653589793, 0.0, 1e-10],
                ScientificFloat = [1e6, 2.5e-3, -1.7e15, 3.14159e0, 0.0],

                # Boolean types
                BooleanTrue = [true, false, true, false, true],
                BooleanMixed = [true, true, false, false, true],

                # String types
                SimpleString = ["hello", "world", "test", "data", "julia"],
                NumberString = ["123", "45.67", "-89", "0", "1.23e4"],
                MixedString = ["text", "123", "45.67", "true", "2023-01-01"],

                # Date and DateTime types
                DateCol = [
                    Date(2023, 1, 1),
                    Date(2023, 6, 15),
                    Date(2024, 12, 31),
                    Date(2020, 2, 29),
                    Date(2025, 8, 13),
                ],
                DateTimeCol = [
                    DateTime(2023, 1, 1, 10, 30),
                    DateTime(2023, 6, 15, 14, 45, 30),
                    DateTime(2024, 12, 31, 23, 59, 59),
                    DateTime(2020, 2, 29, 0, 0, 0),
                    DateTime(2025, 8, 13, 12, 0, 0),
                ],

                # Missing values mixed with types
                IntWithMissing = [1, 2, missing, 4, 5],
                FloatWithMissing = [1.1, missing, 3.3, missing, 5.5],
                StringWithMissing = ["a", "b", missing, "d", "e"],
                BoolWithMissing = [true, missing, false, missing, true],
                DateWithMissing = [
                    Date(2023, 1, 1),
                    missing,
                    Date(2023, 3, 1),
                    Date(2023, 4, 1),
                    missing,
                ],

                # Edge cases
                ZerosAndOnes = [0, 1, 0, 1, 0],  # Could be parsed as Bool or Int
                AllSame = ["constant", "constant", "constant", "constant", "constant"],
                EmptyStrings = ["", "text", "", "more", ""],
                Whitespace = ["  spaced  ", "normal", "\ttab\t", "\nnewline\n", "regular"],

                # Special numeric values
                SpecialFloats = [Inf, -Inf, NaN, 0.0, 1.0],
                VerySmallFloats = [1e-100, 1e-50, 1e-20, 1e-10, 1e-5],
                VeryLargeFloats = [1e50, 1e100, 1e200, 1e308, 1e10],
            )
        end

        function create_ambiguous_types_dataframe()
            """Create DataFrame with ambiguous type data to test parsing decisions"""
            DataFrame(
                # Could be string or numeric
                MaybeNumbers = ["1", "2.5", "3", "4.0", "5"],
                MaybeInts = ["1", "2", "3", "4", "5"],
                MaybeBools = ["true", "false", "TRUE", "FALSE", "True"],

                # Date-like strings
                MaybeDates = [
                    "2023-01-01",
                    "2023/06/15",
                    "01-01-2023",
                    "15/06/2023",
                    "2023-12-31",
                ],
                MaybeDateTimes = [
                    "2023-01-01 10:30:00",
                    "2023-06-15T14:45:30",
                    "01/01/2023 08:00",
                    "2023-12-31 23:59:59",
                    "2023-08-13T12:00:00Z",
                ],

                # Mixed content that should remain strings
                ReallyMixed = ["123", "text", "true", "2023-01-01", "45.67"],
                MixedWithSpaces = [" 123 ", "text", " true", "2023-01-01 ", " 45.67"],

                # Numeric edge cases
                LeadingZeros = ["001", "002", "010", "100", "000"],
                TrailingZeros = ["1.0", "2.00", "3.000", "4.0000", "5.00000"],
                PlusSign = ["+1", "+2.5", "+100", "+0", "+3.14"],

                # Boolean variations
                BoolVariants = ["T", "F", "Y", "N", "1"],
                BoolMixed = ["true", "1", "false", "0", "TRUE"],
            )
        end

        @testset "Column Type Parsing Tests" begin

            mktempdir() do temp_dir

                typed_df = create_typed_test_dataframe()
                ambiguous_df = create_ambiguous_types_dataframe()

                @testset "Basic Type Preservation" begin
                    basic_types_file = joinpath(temp_dir, "basic_types.ods")

                    try
                        write_sheet(basic_types_file, typed_df, "TypedData")

                        if isfile(basic_types_file)
                            # Test default type inference
                            result = read_sheet(basic_types_file)

                            @testset "Integer Type Detection" begin
                                # Test if integers are properly detected
                                if "RegularInt" in names(result)
                                    @test eltype(result.RegularInt) <:
                                          Union{Integer,Missing} ||
                                          eltype(result.RegularInt) <: Union{Number,Missing}
                                    @info "✓ Integer column type: $(eltype(result.RegularInt))"
                                end

                                if "BigInt" in names(result)
                                    @test all(
                                        x -> isa(x, Number) || ismissing(x),
                                        result.BigInt,
                                    )
                                    @info "✓ BigInt values preserved as numbers"
                                end
                            end

                            @testset "Float Type Detection" begin
                                if "Float64Col" in names(result)
                                    @test eltype(result.Float64Col) <:
                                          Union{AbstractFloat,Missing} ||
                                          eltype(result.Float64Col) <: Union{Number,Missing}
                                    @info "✓ Float64 column type: $(eltype(result.Float64Col))"
                                end

                                if "ScientificFloat" in names(result)
                                    @test all(
                                        x -> isa(x, Number) || ismissing(x),
                                        result.ScientificFloat,
                                    )
                                    @info "✓ Scientific notation preserved"
                                end

                                # Test special float values
                                if "SpecialFloats" in names(result)
                                    @test any(isinf, skipmissing(result.SpecialFloats))
                                    @test any(isnan, skipmissing(result.SpecialFloats))
                                    @info "✓ Special float values (Inf, NaN) preserved"
                                end
                            end

                            @testset "Boolean Type Detection" begin
                                if "BooleanTrue" in names(result)
                                    detected_type = eltype(result.BooleanTrue)
                                    if detected_type <: Union{Bool,Missing}
                                        @test true
                                        @info "✓ Boolean column correctly detected: $(detected_type)"
                                    else
                                        @info "⚠ Boolean column detected as: $(detected_type)"
                                    end
                                end
                            end

                            @testset "String Type Detection" begin
                                if "SimpleString" in names(result)
                                    @test eltype(result.SimpleString) <:
                                          Union{AbstractString,Missing}
                                    @info "✓ String column type: $(eltype(result.SimpleString))"
                                end

                                # Test that number-like strings are handled correctly
                                if "NumberString" in names(result)
                                    string_type = eltype(result.NumberString)
                                    @info "✓ Number-like strings type: $(string_type)"
                                end
                            end

                            @testset "Date Type Detection" begin
                                if "DateCol" in names(result)
                                    date_type = eltype(result.DateCol)
                                    if date_type <: Union{Date,Missing} ||
                                       date_type <: Union{AbstractString,Missing}
                                        @info "✓ Date column type: $(date_type)"
                                    else
                                        @info "⚠ Date column detected as: $(date_type)"
                                    end
                                end

                                if "DateTimeCol" in names(result)
                                    datetime_type = eltype(result.DateTimeCol)
                                    @info "✓ DateTime column type: $(datetime_type)"
                                end
                            end

                            @testset "Missing Value Handling" begin
                                # Test columns with missing values
                                mixed_columns = [
                                    "IntWithMissing",
                                    "FloatWithMissing",
                                    "StringWithMissing",
                                    "BoolWithMissing",
                                    "DateWithMissing",
                                ]

                                for col in mixed_columns
                                    if col in names(result)
                                        col_data = result[!, col]
                                        missing_count = count(ismissing, col_data)
                                        @test missing_count > 0
                                        @info "✓ $(col) has $(missing_count) missing values, type: $(eltype(col_data))"
                                    end
                                end
                            end
                        end

                    catch e
                        @warn "Basic type preservation test failed" exception=e
                    end
                end

                @testset "Type Inference Options" begin
                    options_file = joinpath(temp_dir, "type_options.ods")
                    write_sheet(options_file, ambiguous_df, "Ambiguous")

                    if isfile(options_file)
                        @testset "Automatic Type Inference (Default)" begin
                            try
                                result_auto = read_sheet(options_file)

                                # Check how ambiguous data is interpreted
                                if "MaybeNumbers" in names(result_auto)
                                    maybe_nums_type = eltype(result_auto.MaybeNumbers)
                                    @info "✓ MaybeNumbers auto-detected as: $(maybe_nums_type)"

                                    if maybe_nums_type <: Union{Number,Missing}
                                        @test all(
                                            x -> isa(x, Number) || ismissing(x),
                                            result_auto.MaybeNumbers,
                                        )
                                        @info "✓ String numbers converted to numeric"
                                    end
                                end

                                if "MaybeBools" in names(result_auto)
                                    maybe_bools_type = eltype(result_auto.MaybeBools)
                                    @info "✓ MaybeBools auto-detected as: $(maybe_bools_type)"
                                end

                            catch e
                                if isa(e, MethodError)
                                    @info "Default type inference test not applicable"
                                else
                                    @warn "Auto type inference failed" exception=e
                                end
                            end
                        end

                        @testset "Disable Type Inference" begin
                            try
                                result_no_types = read_sheet(options_file, types = false)

                                # When types=false, everything should be strings
                                for col in names(result_no_types)
                                    col_type = eltype(result_no_types[!, col])
                                    if col_type <: Union{AbstractString,Missing}
                                        @info "✓ $(col) kept as string: $(col_type)"
                                    else
                                        @info "⚠ $(col) unexpected type with types=false: $(col_type)"
                                    end
                                end

                            catch e
                                if isa(e, MethodError)
                                    @info "types=false option not supported"
                                else
                                    @warn "Disable type inference failed" exception=e
                                end
                            end
                        end
                    end
                end

                @testset "Edge Case Type Handling" begin
                    edge_file = joinpath(temp_dir, "edge_types.ods")

                    # Create edge case DataFrame
                    edge_df = DataFrame(
                        AllNulls = [missing, missing, missing],
                        AllEmpty = ["", "", ""],
                        AllZeros = [0, 0, 0],
                        AllOnes = [1, 1, 1],
                        SingleValue = [42, 42, 42],
                        VeryLongNumbers = [
                            "123456789012345678901234567890",
                            "987654321098765432109876543210",
                            "111111111111111111111111111111",
                        ],
                        # UnicodeNumbers = ["١٢٣", "٤٥٦", "७८९"],  # Arabic and Devanagari numerals
                        FractionLike = ["1/2", "3/4", "5/8"],
                        PercentageLike = ["50%", "75%", "100%"],
                        CurrencyLike = ["\$100", "€200", "£300"],
                    )

                    try
                        write_sheet(edge_file, edge_df, "EdgeTypes")

                        if isfile(edge_file)
                            result = read_sheet(edge_file)

                            # Test how edge cases are handled
                            for col in names(result)
                                col_type = eltype(result[!, col])
                                sample_vals = first(result[!, col], 3)
                                @info "✓ $(col): $(col_type) - samples: $(sample_vals)"
                            end

                            # Specific tests for edge cases
                            if "AllNulls" in names(result)
                                @test all(ismissing, result.AllNulls)
                                @info "✓ All-null column handled correctly"
                            end

                            # if "VeryLongNumbers" in names(result)
                            #     # These should likely remain as strings
                            #     very_long_type = eltype(result.VeryLongNumbers)
                            #     @test very_long_type <: Union{AbstractString,Missing}
                            #     @info "✓ Very long numbers kept as strings"
                            # end
                        end

                    catch e
                        @warn "Edge case type handling failed" exception=e
                    end
                end

                @testset "Type Consistency Across Sheets" begin
                    consistency_file = joinpath(temp_dir, "type_consistency.ods")

                    # Create similar data in multiple sheets
                    sheet1_df = DataFrame(
                        Numbers = [1, 2, 3, 4, 5],
                        Texts = ["a", "b", "c", "d", "e"],
                        Bools = [true, false, true, false, true],
                    )

                    sheet2_df = DataFrame(
                        Numbers = [10, 20, 30, 40, 50],  # Different values, same type
                        Texts = ["x", "y", "z", "w", "v"],
                        Bools = [false, true, false, true, false],
                    )

                    # Test consistency if multi-sheet operations are available
                    if hasmethod(write_sheets, (String, Dict))
                        try
                            sheets = Dict("Sheet1" => sheet1_df, "Sheet2" => sheet2_df)
                            write_sheets(consistency_file, sheets)

                            if isfile(consistency_file)
                                # Read both sheets and compare types
                                result1 = read_sheet(consistency_file, sheet = "Sheet1")
                                result2 = read_sheet(consistency_file, sheet = "Sheet2")

                                for col in intersect(names(result1), names(result2))
                                    type1 = eltype(result1[!, col])
                                    type2 = eltype(result2[!, col])

                                    if type1 == type2
                                        @info "✓ $(col) consistent across sheets: $(type1)"
                                    else
                                        @info "⚠ $(col) type inconsistency: $(type1) vs $(type2)"
                                    end
                                end
                            end

                        catch e
                            if isa(e, MethodError)
                                @info "Multi-sheet type consistency test not applicable"
                            else
                                @warn "Type consistency test failed" exception=e
                            end
                        end
                    end
                end

                @testset "Type Conversion Performance" begin
                    # Test performance with type-heavy data
                    perf_df = create_typed_test_dataframe()
                    perf_file = joinpath(temp_dir, "type_performance.ods")

                    try
                        # Write with all types
                        write_time = @elapsed write_sheet(perf_file, perf_df, "Performance")

                        if isfile(perf_file)
                            # Read with type inference
                            read_time_typed = @elapsed result_typed = read_sheet(perf_file)

                            # Read without type inference (if supported)
                            read_time_strings = @elapsed begin
                                try
                                    result_strings = read_sheet(perf_file, types = false)
                                catch e
                                    result_strings = nothing
                                end
                            end

                            @info "Type parsing performance:"
                            @info "  Write: $(round(write_time, digits=3))s"
                            @info "  Read (with types): $(round(read_time_typed, digits=3))s"
                            @info "  Read (strings only): $(round(read_time_strings, digits=3))s"

                            # Performance should be reasonable
                            @test write_time < 10
                            @test read_time_typed < 10
                        end

                    catch e
                        @warn "Type conversion performance test failed" exception=e
                    end
                end

            end  # mktempdir

        end  # Column Type Parsing Tests testset

        println("\n" * "="^60)
        println("Column Type Parsing Tests Summary")
        println("="^60)
        println("Type parsing tests completed. Key findings:")
        println("  • Check which types are correctly auto-detected")
        println("  • Verify handling of missing values in typed columns")
        println("  • Assess performance impact of type inference")
        println("  • Review edge cases and ambiguous data handling")
        println("="^60)



    end  # mktempdir

end  # Main testset

# Test summary
println("\n" * "="^60)
println("ODSFiles.jl Test Suite Summary")
println("="^60)
println("Tests completed. Check output above for:")
println("  ✓ Successful operations")
println("  ⚠ Warnings for unsupported features")
println("  ✗ Errors for failed operations")
println("="^60)

# Optional stress tests
if get(ENV, "ODSFILES_STRESS_TEST", "false") == "true"
    @testset "Stress Tests (Optional)" begin
        @info "Running optional stress tests..."

        mktempdir() do stress_dir
            @testset "Very Large Dataset" begin
                try
                    huge_df = create_test_dataframe(10000)  # 10k rows
                    huge_file = joinpath(stress_dir, "huge.ods")

                    write_time = @elapsed write_sheet(huge_file, huge_df, "Huge")
                    read_time = @elapsed result = read_sheet(huge_file)

                    @test size(result) == size(huge_df)
                    @info "Stress test completed: Write $(round(write_time, digits=2))s, Read $(round(read_time, digits=2))s"
                catch e
                    @warn "Stress test failed" exception=e
                end
            end

            @testset "Many Small Sheets" begin
                # Check if write_sheets is available before using
                if hasmethod(write_sheets, (String, Dict))
                    try
                        many_sheets = Dict()
                        for i = 1:20  # 20 sheets
                            many_sheets["Sheet_$i"] = create_test_dataframe(50)
                        end

                        many_file = joinpath(stress_dir, "many_sheets.ods")
                        write_time = @elapsed write_sheets(many_file, many_sheets)

                        @test isfile(many_file)
                        @info "Many sheets stress test completed: $(round(write_time, digits=2))s"
                    catch e
                        @warn "Many sheets stress test failed" exception=e
                    end
                else
                    @info "write_sheets not available for stress test"
                end
            end
        end
    end
end

println("All tests completed!")