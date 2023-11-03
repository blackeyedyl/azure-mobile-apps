// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Licensed under the MIT License.

using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using System.IO;
using System.Text;

namespace Microsoft.AspNetCore.Datasync.CosmosDb;

/// <summary>
/// Implements the recommended <see cref="CosmosSerializer"/> that works with the Datasync SDK. Specifically,
/// this helps with the following converting <see cref="DateTimeOffset"/> to ISO8601 format, because the default
/// Datasync SDK serializer prefers the interchange format 'yyyy-MM-dd'T'HH:mm:ss.fffK' format which doesn't
/// play well with Cosmos DB.
/// </summary>
public class CosmosDatasyncSerializer : CosmosSerializer
{
    private static readonly Encoding DefaultEncoding = new UTF8Encoding(false, true);
    private readonly CosmosSerializationOptions cosmosSerializerOptions;
    private readonly JsonSerializerSettings jsonSerializerSettings;

    public CosmosDatasyncSerializer(
        CosmosSerializationOptions cosmosSerializerOptions = null,
        JsonSerializerSettings jsonSerializerSettings = null)
    {
        this.cosmosSerializerOptions = cosmosSerializerOptions ?? new CosmosSerializationOptions();
        this.jsonSerializerSettings = jsonSerializerSettings ?? CreateDefaultSettings();
    }

    /// <summary>
    /// Convert a Stream to the passed in type.
    /// </summary>
    /// <typeparam name="T">The type of object that should be deserialized</typeparam>
    /// <param name="stream">An open stream that is readable that contains JSON</param>
    /// <returns>The object representing the deserialized stream</returns>
    public override T FromStream<T>(Stream stream)
    {
        using (stream)
        {
            if (typeof(Stream).IsAssignableFrom(typeof(T)))
            {
                return (T)(object)stream;
            }

            using StreamReader sr = new StreamReader(stream);
            using JsonTextReader jsonTextReader = new JsonTextReader(sr);
            JsonSerializer jsonSerializer = GetSerializer();
            return jsonSerializer.Deserialize<T>(jsonTextReader);
        }
    }

    /// <summary>
    /// Converts an object to a open readable stream
    /// </summary>
    /// <typeparam name="T">The type of object being serialized</typeparam>
    /// <param name="input">The object to be serialized</param>
    /// <returns>An open readable stream containing the JSON of the serialized object</returns>
    public override Stream ToStream<T>(T input)
    {
        MemoryStream streamPayload = new MemoryStream();
        using var streamWriter = new StreamWriter(streamPayload, encoding: DefaultEncoding, bufferSize: 1024, leaveOpen: true);

        using var writer = new JsonTextWriter(streamWriter);
        writer.Formatting = Formatting.None;
        JsonSerializer jsonSerializer = GetSerializer();
        jsonSerializer.Serialize(writer, input);
        writer.Flush();
        streamWriter.Flush();

        streamPayload.Position = 0;
        return streamPayload;
    }

    /// <summary>
    /// Creates the recommended default <see cref="JsonSerializationSetting"/>s for the Cosmos SDK.
    /// </summary>
    /// <returns></returns>
    private JsonSerializerSettings CreateDefaultSettings()
    {
        JsonSerializerSettings defaultSettings = new()
        {
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DefaultValueHandling = DefaultValueHandling.Include,
            NullValueHandling = cosmosSerializerOptions.IgnoreNullValues ? NullValueHandling.Ignore : NullValueHandling.Include,
            Formatting = cosmosSerializerOptions.Indented ? Formatting.Indented : Formatting.None,
            ContractResolver = cosmosSerializerOptions.PropertyNamingPolicy == CosmosPropertyNamingPolicy.CamelCase
                    ? new CamelCasePropertyNamesContractResolver()
                    : null,
            MaxDepth = 64, // https://github.com/advisories/GHSA-5crp-9r3c-p9vr
        };
        defaultSettings.Converters.Add(new CosmosDateTimeOffsetJsonConverter());
        defaultSettings.Converters.Add(new StringEnumConverter());
        return defaultSettings;
    }

    /// <summary>
    /// JsonSerializer has hit a race conditions with custom settings that cause null reference exception.
    /// To avoid the race condition a new JsonSerializer is created for each call
    /// </summary>
    private JsonSerializer GetSerializer()
    {
        return JsonSerializer.Create(jsonSerializerSettings);
    }
}