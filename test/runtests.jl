using Test
using ODSFiles
using DataFrames

@testset "ODSFiles.jl Tests" begin
    @testset "Basic functionality" begin
        # Create test data
        df = DataFrame(A=[1,2,3], B=["x","y","z"])
        
        # Test writing
        write_sheet("test.ods", df, "TestSheet")
        @test isfile("test.ods")
        
        # Test reading
        df_read = read_sheet("test.ods")
        @test size(df_read) == size(df)
        @test names(df_read) == names(df)
        
        # Cleanup
        rm("test.ods")
    end
end