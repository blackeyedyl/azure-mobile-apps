// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using System.Globalization;

namespace Microsoft.AspNetCore.Datasync.CosmosDb.Test;

[ExcludeFromCodeCoverage]
public class BuildPartitionKey_Tests
{
    public class Entity : CosmosTableData
    {
        public bool BoolProperty { get; set; } = true;
        public byte ByteProperty { get; set; } = 6;
        public DateTime DateTimeProperty { get; set; } = DateTime.Now;
        public decimal DecimalProperty { get; set; } = 1.1m;
        public double DoubleProperty { get; set; } = 999;
        public float FloatProperty { get; set; } = 1.3f;
        public Guid GuidProperty { get; set; } = Guid.NewGuid();
        public int IntProperty { get; set; } = 4;
        public long LongProperty { get; set; } = 3L;
        public string NullProperty { get; set; }
        public sbyte SByteProperty { get; set; } = 7;
        public short ShortProperty { get; set; } = 5;
        public string StringProperty { get; set; } = Guid.NewGuid().ToString("N");
        public uint UIntProperty { get; set; } = 9;
        public ulong ULongProperty { get; set; } = 10;
        public ushort UShortProperty { get; set; } = 8;
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromBytePropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.ByteProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.ByteProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDecimalFromDecimalPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.DecimalProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(Convert.ToDouble(entity.DecimalProperty, CultureInfo.InvariantCulture))
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromDoublePropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.DoubleProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.DoubleProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromFloatPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.FloatProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.FloatProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromIntPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.IntProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.IntProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromLongPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.LongProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.LongProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromSBytePropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.SByteProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.SByteProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromShortPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.ShortProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.ShortProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromUIntPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.UIntProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.UIntProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedDoubleFromULongPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.ULongProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.ULongProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedBoolPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.BoolProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.BoolProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedStringPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.StringProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.StringProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedStringFromNonStringPropertyPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.DateTimeProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.DateTimeProperty.ToString())
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithValidData_ReturnsExpectedHierarchicialPartitionKey()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { nameof(entity.StringProperty), nameof(entity.BoolProperty), nameof(entity.DoubleProperty) };
        var expectedPartitionKey = new PartitionKeyBuilder()
            .Add(entity.StringProperty)
            .Add(entity.BoolProperty)
            .Add(entity.DoubleProperty)
            .Build();

        // Act
        var partitionKey = entity.BuildPartitionKey(propertyNames);

        // Assert
        Assert.Equal(expectedPartitionKey, partitionKey);
    }

    [Fact]
    public void WithNullPropertyNames_ThrowsArgumentNullException()
    {
        // Arrange
        Entity entity = new();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => entity.BuildPartitionKey(null));
    }

    [Fact]
    public void WithEmptyPropertyNames_ThrowsArgumentException()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string>();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => entity.BuildPartitionKey(propertyNames));
    }

    [Fact]
    public void WithInvalidPropertyName_ThrowsArgumentException()
    {
        // Arrange
        Entity entity = new();
        var propertyNames = new List<string> { "InvalidProperty" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => entity.BuildPartitionKey(propertyNames));
    }

    [Fact]
    public void WithNullPropertyValue_ThrowsArgumentNullException()
    {
        // Arrange
        Entity entity = new();
        List<string> propertyNames = new() { nameof(entity.NullProperty) };


        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => entity.BuildPartitionKey(propertyNames));
    }
}