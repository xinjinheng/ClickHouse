#include <DataTypes/Serializations/SerializationVector.h>
#include <DataTypes/DataTypeVector.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/Exception.h>

namespace DB
{

void SerializationVector::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const Vector & vector = get<Vector>(field);
    size_t size = vector.size();
    writeVarUInt(size, ostr);
    for (const auto & elem : vector)
        writeDoubleBinary(elem, ostr);
}

void SerializationVector::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    size_t size;
    readVarUInt(size, istr);
    Vector vector;
    vector.resize(size);
    for (size_t i = 0; i < size; ++i)
        readDoubleBinary(vector[i], istr);
    field = std::move(vector);
}

void SerializationVector::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const Vector & vector = col.getData()[row_num];
    size_t size = vector.size();
    writeVarUInt(size, ostr);
    for (const auto & elem : vector)
        writeDoubleBinary(elem, ostr);
}

void SerializationVector::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ColumnVector<Vector> & col = assert_cast<ColumnVector<Vector> &>(column);
    size_t size;
    readVarUInt(size, istr);
    Vector vector;
    vector.resize(size);
    for (size_t i = 0; i < size; ++i)
        readDoubleBinary(vector[i], istr);
    col.getData().push_back(std::move(vector));
}

void SerializationVector::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const Vector & vector = col.getData()[row_num];

    writeChar('(', ostr);
    for (size_t i = 0; i < vector.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        writeDoubleText(vector[i], ostr);
    }
    writeChar(')', ostr);
}

void SerializationVector::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    ColumnVector<Vector> & col = assert_cast<ColumnVector<Vector> &>(column);
    Vector vector;

    assertChar('(', istr);
    while (!checkChar(')', istr))
    {
        if (!vector.empty())
            assertChar(',', istr);
        double value;
        readDoubleText(value, istr);
        vector.push_back(value);
    }

    col.getData().push_back(std::move(vector));
}

bool SerializationVector::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    try
    {
        deserializeText(column, istr, settings, whole);
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

void SerializationVector::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

void SerializationVector::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeText(column, istr, settings);
}

bool SerializationVector::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeText(column, istr, settings);
}

void SerializationVector::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationVector::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    assertChar('"', istr);
    deserializeText(column, istr, settings);
    assertChar('"', istr);
}

bool SerializationVector::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        if (!checkChar('"', istr))
            return false;
        if (!tryDeserializeText(column, istr, settings))
            return false;
        if (!checkChar('"', istr))
            return false;
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

void SerializationVector::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationVector::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (checkChar('"', istr))
    {
        deserializeText(column, istr, settings);
        assertChar('"', istr);
    }
    else
    {
        deserializeText(column, istr, settings);
    }
}

bool SerializationVector::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        if (checkChar('"', istr))
        {
            if (!tryDeserializeText(column, istr, settings))
                return false;
            if (!checkChar('"', istr))
                return false;
        }
        else
        {
            if (!tryDeserializeText(column, istr, settings))
                return false;
        }
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

void SerializationVector::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const Vector & vector = col.getData()[row_num];

    writeChar('[', ostr);
    for (size_t i = 0; i < vector.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        writeDoubleText(vector[i], ostr);
    }
    writeChar(']', ostr);
}

void SerializationVector::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnVector<Vector> & col = assert_cast<ColumnVector<Vector> &>(column);
    Vector vector;

    assertChar('[', istr);
    while (!checkChar(']', istr))
    {
        if (!vector.empty())
            assertChar(',', istr);
        double value;
        readDoubleText(value, istr);
        vector.push_back(value);
    }

    col.getData().push_back(std::move(vector));
}

bool SerializationVector::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        deserializeTextJSON(column, istr, settings);
        return true;
    }
    catch (const Exception &)
    {
        return false;
    }
}

void SerializationVector::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const Vector & vector = col.getData()[row_num];

    writeChar('[', ostr);
    writeChar('\n', ostr);

    for (size_t i = 0; i < vector.size(); ++i)
    {
        writeCString(std::string(indent + 4, ' ').c_str(), ostr);
        writeDoubleText(vector[i], ostr);
        if (i != vector.size() - 1)
            writeChar(',', ostr);
        writeChar('\n', ostr);
    }

    writeCString(std::string(indent, ' ').c_str(), ostr);
    writeChar(']', ostr);
}

void SerializationVector::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const PaddedPODArray<Vector> & data = col.getData();

    size_t end = offset + limit;
    for (size_t i = offset; i < end; ++i)
    {
        const Vector & vector = data[i];
        size_t size = vector.size();
        writeVarUInt(size, ostr);
        for (const auto & elem : vector)
            writeDoubleBinary(elem, ostr);
    }
}

void SerializationVector::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    ColumnVector<Vector> & col = assert_cast<ColumnVector<Vector> &>(column);
    PaddedPODArray<Vector> & data = col.getData();

    size_t start = data.size();
    data.resize(start + limit);

    for (size_t i = start; i < start + limit; ++i)
    {
        size_t size;
        readVarUInt(size, istr);
        Vector & vector = data[i];
        vector.resize(size);
        for (size_t j = 0; j < size; ++j)
            readDoubleBinary(vector[j], istr);
    }
}

void SerializationVector::serializeBinaryBulkWithMultipleStreams(const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr &) const
{
    const ColumnVector<Vector> & col = assert_cast<const ColumnVector<Vector> &>(column);
    const PaddedPODArray<Vector> & data = col.getData();

    size_t end = offset + limit;
    for (size_t i = offset; i < end; ++i)
    {
        const Vector & vector = data[i];
        size_t size = vector.size();
        writeVarUInt(size, settings.getStream(0));
        for (const auto & elem : vector)
            writeDoubleBinary(elem, settings.getStream(0));
    }
}

void SerializationVector::deserializeBinaryBulkWithMultipleStreams(IColumn & column, size_t & rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr &, UncompressedCache *) const
{
    ColumnVector<Vector> & col = assert_cast<ColumnVector<Vector> &>(column);
    PaddedPODArray<Vector> & data = col.getData();

    size_t start = data.size();
    data.resize(start + limit);

    for (size_t i = start; i < start + limit; ++i)
    {
        size_t size;
        readVarUInt(size, settings.getStream(0));
        Vector & vector = data[i];
        vector.resize(size);
        for (size_t j = 0; j < size; ++j)
            readDoubleBinary(vector[j], settings.getStream(0));
    }

    rows_offset += limit;
}

}
