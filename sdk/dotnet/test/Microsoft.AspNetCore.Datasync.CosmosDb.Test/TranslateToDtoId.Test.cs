namespace Microsoft.AspNetCore.Datasync.CosmosDb.Test;

public class TranslateToDtoId_Test
{
    [Fact]
    public void ThrowsArgumentNullException_WhenPartitionKeyPropertyNamesIsNull()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1" };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => movie.TranslateToDtoId(null));
    }

    [Fact]
    public void ThrowsArgumentException_WhenPartitionKeyPropertyNamesIsEmpty()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => movie.TranslateToDtoId(new List<string>()));
    }

    [Fact]
    public void ThrowsArgumentException_WhenInvalidPropertyName()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1", Title = "Test" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => movie.TranslateToDtoId(new List<string> { "InvalidProperty" }));
    }

    [Fact]
    public void ThrowsArgumentNullException_WhenPropertyIsNull()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1", Title = null };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => movie.TranslateToDtoId(new List<string> { "Title" }));
    }

    [Fact]
    public void ReturnsCorrectValue_WhenValidInputs()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1", Title = "Test" };

        // Act
        var result = movie.TranslateToDtoId(new List<string> { "Title" });

        // Assert
        Assert.Equal("1:Test", result);
    }

    [Fact]
    public void ReturnsCorrectValue_WhenMultipleValidInputs()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1", Title = "Test", Rating = "R", Year = 1999 };

        // Act
        var result = movie.TranslateToDtoId(new List<string> { "Rating", "Year" });

        // Assert
        Assert.Equal("1:R|1999", result);
    }

    [Fact]
    public void ReturnsId_WhenPartitionKeyIsId()
    {
        // Arrange
        var movie = new CosmosMovie { Id = "1" };

        // Act
        var result = movie.TranslateToDtoId(new List<string> { "Id" });

        // Assert
        Assert.Equal("1", result);
    }
}
