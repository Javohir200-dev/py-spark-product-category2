from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_dataframes(spark):
    # Создание датафреймов продуктов
    products = spark.createDataFrame([
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
        (4, "Product D")
    ], ["product_id", "product_name"])

    # Создание датафреймов категорий
    categories = spark.createDataFrame([
        (1, "Category X"),
        (2, "Category Y"),
        (3, "Category Z")
    ], ["category_id", "category_name"])

    # Создание датафреймов связей продуктов и категорий
    product_categories = spark.createDataFrame([
        (1, 1),
        (1, 2),
        (2, 3),
        (3, 1)
    ], ["product_id", "category_id"])

    return products, categories, product_categories

def get_product_category_pairs_and_products_without_category(products, categories, product_categories):
    # Объединение датафреймов для получения пар "Имя продукта - Имя категории"
    product_category_pairs = products.join(
        product_categories, on="product_id", how="left"
    ).join(
        categories, on="category_id", how="left"
    ).select(
        col("product_name"), col("category_name")
    )

    # Получение продуктов без категорий
    products_without_category = product_category_pairs.filter(
        col("category_name").isNull()
    ).select(
        col("product_name")
    )

    return product_category_pairs, products_without_category

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductCategoryApp") \
        .getOrCreate()

    products, categories, product_categories = create_dataframes(spark)
    product_category_pairs, products_without_category = get_product_category_pairs_and_products_without_category(products, categories, product_categories)

    print("Product - Category Pairs:")
    product_category_pairs.show()

    print("Products without Category:")
    products_without_category.show()

    spark.stop()
