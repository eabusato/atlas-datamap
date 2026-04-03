"""Semantic inference prompt templates."""


TABLE_PROMPT_TEMPLATE = """You are a specialist data engineer. Analyze this database table structure to infer its business meaning.

TABLE CONTEXT:
Table: {schema}.{table_name}
Physical Type: {table_type}
Estimated Rows: {row_count}
Main Columns: {top_columns_summary}
Declared Relationships: {fk_summary}
Previous Heuristic Classification: {heuristic_classification}

INSTRUCTIONS:
1. Reply STRICTLY with a valid JSON object, without markdown or conversational preamble.
2. Infer the table's primary business domain.
3. Use the previous heuristic classification when it is not "unknown".
4. Do not invent business rules not reflected in names or relationships.

REQUIRED JSON RESPONSE:
{{
  "short_description": "Short table summary",
  "detailed_description": "Technical explanation of the table purpose",
  "probable_domain": "Primary business domain",
  "probable_role": "Primary structural role",
  "confidence": 0.0
}}"""


COLUMN_PROMPT_TEMPLATE = """You are a specialist data engineer. Analyze this database column metadata and sanitized examples to infer its semantic role.

COLUMN CONTEXT:
Parent Table: {schema}.{table_name}
Parent Table Summary: {table_short_description}
Parent Table Details: {table_detailed_description}
Parent Table Role: {table_role}
Nearby Columns: {sibling_columns}
Column: {column_name}
Canonical Type: {canonical_type}
Native Type: {native_type}
Column Comment: {comment}
Nullable: {nullable}
Indexed: {is_indexed}
Unique: {is_unique}
Distinct Values: {distinct}
Null Rate: {null_rate}
Average Length: {avg_length}
Detected Pattern: {pattern}
Sample Summary: {sample_summary}
Sanitized Samples: {samples}

INSTRUCTIONS:
1. Reply STRICTLY with a valid JSON object, without markdown or conversational preamble.
2. Infer the exact functional role of the column from the parent table meaning, sibling columns, metadata, pattern and samples.
3. Prefer domain-specific wording when the table or column names make the entity clear.
4. For long text fields, decide whether the content is a story, article, post, comment, profile bio, message, document body, or another specific text artifact.
5. Avoid vague catch-all wording such as "messages, emails, or documents" unless the metadata is truly ambiguous.
6. Do not invent hidden business rules.

REQUIRED JSON RESPONSE:
{{
  "short_description": "Short column summary",
  "detailed_description": "Technical explanation of the stored data",
  "probable_role": "Functional role of the column",
  "confidence": 0.0
}}"""
