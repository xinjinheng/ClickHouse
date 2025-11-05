#include <DataTypes/DataTypeVector.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Common/assert_cast.h>

namespace DB
{

DataTypeVector::DataTypeVector(size_t dimension_)
    : dimension(dimension_)
{}

std::string DataTypeVector::getName() const
{
    return fmt::format("Vector({})", dimension);
}

Field DataTypeVector::getDefault() const
{
    return Array(dimension, 0.0);
}

bool DataTypeVector::equals(const IDataType & rhs) const
{
    if (const auto * other = typeid_cast<const DataTypeVector *>(&rhs))
        return dimension == other->dimension;
    return false;
}

ColumnPtr DataTypeVector::createColumn() const
{
    return ColumnVector<Array>::create();
}

ColumnPtr DataTypeVector::createColumnConst(size_t size, const Field & field) const
{
    const Array & array = get<Array>(field);
    if (array.size() != dimension)
        throw Exception(ErrorCodes::ARRAY_SIZE_DOESNT_MATCH, "Array size doesn't match vector dimension: expected {}, got {}", dimension, array.size());
    
    return ColumnVector<Array>::createConst(size, array);
}

SerializationPtr DataTypeVector::doGetDefaultSerialization() const
{
    class SerializationVector : public Serialization
    {
    public:
        explicit SerializationVector(size_t dimension_)
            : dimension(dimension_)
        {}

        void serializeBinary(const Field & field, WriteBuffer & ostr) const override
        {
            const Array & array = get<Array>(field);
            if (array.size() != dimension)
                throw Exception(ErrorCodes::ARRAY_SIZE_DOESNT_MATCH, "Array size doesn't match vector dimension: expected {}, got {}", dimension, array.size());
            
            writeVarUInt(dimension, ostr);
            for (const auto & elem : array)
                writeBinary(get<Float64>(elem), ostr);
        }

        void deserializeBinary(Field & field, ReadBuffer & istr) const override
        {
            size_t read_dimension;
            readVarUInt(read_dimension, istr);
            if (read_dimension != dimension)
                throw Exception(ErrorCodes::ARRAY_SIZE_DOESNT_MATCH, "Array size doesn't match vector dimension: expected {}, got {}", dimension, read_dimension);
            
            Array array(read_dimension);
            for (size_t i = 0; i < read_dimension; ++i)
            {
                Float64 value;
                readBinary(value, istr);
                array[i] = value;
            }
            field = std::move(array);
        }

        void serializeText(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override
        {
            const Array & array = get<Array>(field);
            writeChar('[', ostr);
            for (size_t i = 0; i < array.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', ostr);
                writeText(get<Float64>(array[i]), ostr);
            }
            writeChar(']', ostr);
        }

        void deserializeText(Field & field, ReadBuffer & istr, const FormatSettings &) const override
        {
            skipWhitespaceIfAny(istr);
            if (!checkChar('[', istr))
                throw Exception(ErrorCodes::CANNOT_PARSE_ARRAY, "Expected '[' at begin of vector");
            
            Array array;
            while (true)
            {
                skipWhitespaceIfAny(istr);
                if (checkChar(']', istr))
                    break;
                
                if (!array.empty())
                {
                    skipWhitespaceIfAny(istr);
                    if (!checkChar(',', istr))
                        throw Exception(ErrorCodes::CANNOT_PARSE_ARRAY, "Expected ',' between vector elements");
                    skipWhitespaceIfAny(istr);
                }
                
                Float64 value;
                readText(value, istr);
                array.push_back(value);
            }
            
            if (array.size() != dimension)
                throw Exception(ErrorCodes::ARRAY_SIZE_DOESNT_MATCH, "Array size doesn't match vector dimension: expected {}, got {}", dimension, array.size());
            
            field = std::move(array);
        }

        void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
        {
            const ColumnVector<Array> & col = assert_cast<const ColumnVector<Array> &>(column);
            const typename ColumnVector<Array>::Container & data = col.getData();
            
            size_t to = std::min(offset + limit, data.size());
            for (size_t i = offset; i < to; ++i)
            {
                const Array & array = data[i];
                if (array.size() != dimension)
                    throw Exception(ErrorCodes::ARRAY_SIZE_DOESNT_MATCH, "Array size doesn't match vector dimension: expected {}, got {}", dimension, array.size());
                
                for (const auto & elem : array)
                    writeBinary(get<Float64>(elem), ostr);
            }
        }

        void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double) const override
        {
            ColumnVector<Array> & col = assert_cast<ColumnVector<Array> &>(column);
            typename ColumnVector<Array>::Container & data = col.getData();
            
            size_t size = 0;
            while (size < limit && !istr.eof())
            {
                Array array(dimension);
                for (size_t i = 0; i < dimension; ++i)
                {
                    Float64 value;
                    readBinary(value, istr);
                    array[i] = value;
                }
                data.push_back(std::move(array));
                ++size;
            }
        }

    private:
        size_t dimension;
    };

    return std::make_shared<SerializationVector>(dimension);
}

}