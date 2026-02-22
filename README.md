# DataOps Pipeline

dataPiplineTestsQuality/
├─ dags/
│  └─ sales_pipeline_dag.py
├─ src/
│  └─ transformations.py
├─ tests/
│  ├─ test_transformations.py
│  └─ test_e2e.py
├─ .github/workflows
│  └─ ci.yml
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
└─ README.md
 
Цель: построить конвейер, который загружает сырые данные, валидирует, трансформирует и сохраняет очищенный датасет для аналитики.

## Источник данных

Используется CSV:

- `https://github.com/novadataops/DataOps-Final-Project/raw/main/raw_data_final_project_1/raw_sales.csv` [page:1, {ts:0}]

Схема:

- order_id (int)
- order_date (строка с датой)
- customer_id (str)
- product_id (str)
- quantity (int/float)
- price (int/float)
- region (str)

## Бизнес‑логика

- Проверка типов данных.
- Фильтрация по допустимым регионам: eu, us, APAC.
- Удаление записей с некорректной датой, количеством, ценой.
- Удаление дубликатов по `order_id`.

## Data Quality

### При загрузке (ingress)

- Проверка схемы и полного набора колонок.
- Проверка ненулевых значений для ключевых полей.
- Проверка типов для `order_id`, `quantity`, `price`.
- При нарушении — падение таски Airflow.

### После трансформаций

- Отсутствие дубликатов `order_id`.
- `quantity > 0`, `price > 0`
- Значения `region` входят в {eu, us, APAC}.

При нарушении — таска `validate_clean_data` фейлится, конвейер останавливается.

## Тестирование

- `tests/test_transformations.py` — юнит‑тесты бизнес‑логики.
- `tests/test_end_to_end.py` — end‑to‑end тест всего пайплайна на реальном CSV.

## Запуск
1. Клонировать репозиторий

```bash
git clone <url_этого_репозитория>.git
cd <каталог_репозитория>
```
2. Собрать и запустить окружение

```bash
docker-compose up --build
```
3. Проверка доступности Airflow Web UI

```
http://localhost:8080
```
4. Зайти в UI, активировать DAG и вручную запустить run.
