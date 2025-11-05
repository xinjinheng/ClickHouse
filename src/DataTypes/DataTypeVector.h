#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <vector>

namespace DB
{

/**
 * 向量数据类型，用于存储多模态数据的特征向量
 * 支持不同维度的浮点向量
 */
class DataTypeVector final : public IDataType
{
public:
    static constexpr auto family_name = "Vector";

    explicit DataTypeVector(size_t dimension_);

    std::string getName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Vector; }
    size_t getDimension() const { return dimension; }

    bool canBeInsideNullable() const override { return true; }

    Field getDefault() const override;
    bool equals(const IDataType & rhs) const override;
    bool isComparable() const override { return false; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isCategorial() const override { return false; }

    ColumnPtr createColumn() const override;
    ColumnPtr createColumnConst(size_t size, const Field & field) const override;

    SerializationPtr doGetDefaultSerialization() const override;

private:
    size_t dimension;
};

}