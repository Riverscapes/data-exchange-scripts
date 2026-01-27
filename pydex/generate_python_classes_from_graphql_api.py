"""
Generate Python TypedDict definitions from a GraphQL schema.

This script reads the project's 'graphql.config.json' to locate the schema file,
parses it, and generates Python `TypedDict` classes for all InputObjects.
This allows for type-safe construction of GraphQL mutation payloads.
"""

import argparse
from pathlib import Path

from graphql import (
    InputObjectTypeDefinitionNode,
    ListTypeNode,
    NamedTypeNode,
    NonNullTypeNode,
    TypeNode,
    parse,
)


def get_python_type(type_node: TypeNode) -> str:
    """
    Recursively resolve GraphQL types to modern Python type strings.

    Args:
        type_node: The GraphQL AST node representing the type.

    Returns:
        A string representing the Python type (e.g., 'list[str]', 'int').
    """
    if isinstance(type_node, NonNullTypeNode):
        return get_python_type(type_node.type)

    if isinstance(type_node, ListTypeNode):
        inner_type = get_python_type(type_node.type)
        return f"list[{inner_type}]"

    if isinstance(type_node, NamedTypeNode):
        name = type_node.name.value
        mapping = {
            'String': 'str',
            'ID': 'str',
            'Boolean': 'bool',
            'Int': 'int',
            'Float': 'float'
        }
        # Use quotes for forward references to other classes
        return mapping.get(name, f"'{name}'")

    return "Any"


def generate_types(schema_path: Path, output_path: Path) -> None:
    """
    Parse the schema and write Python TypedDict definitions to a file.

    Args:
        schema_path: Path to the .graphql schema file.
        output_path: Path to the output .py file.
    """
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        return

    print(f"Reading schema from: {schema_path}")
    print(f"Writing types to:    {output_path}")

    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_content = f.read()

    doc = parse(schema_content)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f'"""\nGenerated from {schema_path.name} using {Path(__file__).name}\n"""\n')
        f.write("from typing import TypedDict, Any\n\n")

        count = 0
        for definition in doc.definitions:
            # We focus on Input types as they are critical for constructing mutation payloads
            if isinstance(definition, InputObjectTypeDefinitionNode):
                count += 1
                name = definition.name.value
                f.write(f"class {name}(TypedDict, total=False):\n")

                if not definition.fields:
                    f.write("    pass\n\n")
                    continue

                for field in definition.fields:
                    field_name = field.name.value
                    python_type = get_python_type(field.type)
                    f.write(f"    {field_name}: {python_type}\n")
                f.write("\n")
    
    print(f"Successfully generated {count} types.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Python TypedDicts from GraphQL Schema")

    default_schema = Path("pydex/graphql/riverscapes.schema.graphql")
    default_output = Path("pydex/graphql/generated_types.py")

    parser.add_argument('--schema', type=Path, default=default_schema,
                        help='Path to riverscapes.schema.graphql')
    parser.add_argument('--output', type=Path, default=default_output,
                        help='Path to output .py file')

    args = parser.parse_args()
    generate_types(args.schema, args.output)
