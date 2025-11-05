#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnFloat64.h>
#include <Common/NaNUtils.h>

#include <cmath>
#include <numeric>

namespace DB
{

struct CosineSimilarityName { static constexpr auto name = "cosineSimilarity"; };

class FunctionCosineSimilarity : public IFunction
{
public:
    static constexpr auto name = CosineSimilarityName::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionCosineSimilarity>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeVector * vector_type1 = typeid_cast<const DataTypeVector *>(arguments[0].get());
        const DataTypeVector * vector_type2 = typeid_cast<const DataTypeVector *>(arguments[1].get());

        if (!vector_type1 || !vector_type2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Both arguments must be Vector type");

        if (vector_type1->getDimension() != vector_type2->getDimension())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Vectors must have the same dimension");

        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnVector<Vector> * col1 = checkAndGetColumn<ColumnVector<Vector>>(&*arguments[0].column);
        const ColumnVector<Vector> * col2 = checkAndGetColumn<ColumnVector<Vector>>(&*arguments[1].column);

        auto result = ColumnFloat64::create();
        ColumnFloat64::Container & data = result->getData();
        data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Vector & vec1 = col1->getData()[i];
            const Vector & vec2 = col2->getData()[i];

            double dot_product = 0.0;
            double norm1 = 0.0;
            double norm2 = 0.0;

            for (size_t j = 0; j < vec1.size(); ++j)
            {
                dot_product += vec1[j] * vec2[j];
                norm1 += vec1[j] * vec1[j];
                norm2 += vec2[j] * vec2[j];
            }

            if (norm1 == 0.0 || norm2 == 0.0)
                data[i] = std::nan("");
            else
                data[i] = dot_product / (std::sqrt(norm1) * std::sqrt(norm2));
        }

        return result;
    }
};

REGISTER_FUNCTION(CosineSimilarity)
{
    factory.registerFunction<FunctionCosineSimilarity>();
}
}