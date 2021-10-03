import csv
import logging
import random
from enum import Enum, unique
from pathlib import Path

from fire import Fire


logger = logging.getLogger(__name__)
logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

HEX_CHARS = 'ABCDEF'
STR_CHARS = 'abcdefghijklmnopqrstuvwxyz '
HEX_LEN = 8


@unique
class ColumnType(Enum):
    INT = 'int'
    STR = 'str'
    HEX = 'hex'


def random_type():
    return random.choice(list(ColumnType))


def random_value(col_type: ColumnType):
    if col_type is ColumnType.INT:
        return random.randint(-100, 100)
    elif col_type is ColumnType.STR:
        length = random.randint(0, 25)
        return ''.join(random.choices(STR_CHARS, k=length))
    elif col_type is ColumnType.HEX:
        return ''.join(random.choices(HEX_CHARS, k=HEX_LEN))
    else:
        raise ValueError(f'Unknown type {col_type}')


def read_key_value(encoded: str, delimiter: str, default_value=None):
    if delimiter in encoded:
        key, value = encoded.split(delimiter)
        value = value.strip()
    else:
        key, value = encoded, default_value
    return key.strip(), value


def generate_row(column_list, column_types, condition_values=None):
    condition_values = condition_values or {}
    result = []
    for col_name in column_list:
        if col_name in condition_values:
            result.append(condition_values[col_name])
        else:
            result.append(random_value(column_types[col_name]))
    return result


def generate_table(n_rows, out_file, out_file_correct, column_list, column_types, condition_values, condition_freq,
                   guaranteed):
    with open(out_file, 'w', newline='') as main_file, open(out_file_correct, 'w', newline='') as correct_file:
        main_writer = csv.writer(main_file)
        correct_writer = csv.writer(correct_file)
        main_writer.writerow(column_list)
        correct_writer.writerow(column_list)
        had_correct_rows = False
        for _ in range(n_rows - 1 if guaranteed else n_rows):
            if random.random() < condition_freq:
                row = generate_row(column_list, column_types, condition_values)
                correct_writer.writerow(row)
                had_correct_rows = True
            else:
                row = generate_row(column_list, column_types)
            main_writer.writerow(row)
        if guaranteed:
            if random.random() < condition_freq or not had_correct_rows:
                row = generate_row(column_list, column_types, condition_values)
                correct_writer.writerow(row)
            else:
                row = generate_row(column_list, column_types)
            main_writer.writerow(row)


def main(n_rows, out_file, out_file_correct=None, columns=None, n_columns=None, seed=None, conditions=None,
         condition_frequency=0.1, guaranteed=False):
    if columns is None and n_columns is None:
        raise ValueError('Must specify either columns or n_columns argument')
    if seed is None:
        random.seed()
    else:
        random.seed(seed)
    column_list = []
    column_types = {}
    if columns is not None:
        for token in columns.split(','):
            name, col_type = read_key_value(token, ':', default_value='any')
            if name == '':
                raise ValueError('Column name cannot be an empty string')
            if col_type == 'any':
                col_type = random_type()
            else:
                col_type = ColumnType(col_type)
            if name in column_types:
                raise ValueError(f'Column name {name} is written twice')
            column_list.append(name)
            column_types[name] = col_type
    if n_columns is not None:
        if len(column_list) > n_columns:
            raise ValueError(f'Number of columns is set to {n_columns}, got {len(column_list)} instead')
        elif len(column_list) < n_columns:
            for i in range(len(column_list), n_columns):
                col_name = f'column_{i}'
                while col_name in column_types:
                    col_name = col_name + '_'
                column_list.append(col_name)
                column_types[col_name] = random_type()
    condition_values = {}
    if conditions is not None:
        for token in conditions.split(','):
            col_name, value = read_key_value(token, '=', default_value=None)
            if col_name not in column_types:
                raise ValueError(f'Unknown column {col_name}')
            if value is None:
                value = random_value(column_types[col_name])
            # TODO: check value type?
            condition_values[col_name] = value

    out_file = Path(out_file)
    if out_file_correct is None:
        out_file_correct = out_file.parent / f'{out_file.stem}_correct.{out_file.suffix}'
    else:
        out_file_correct = Path(out_file_correct)
    generate_table(
        n_rows,
        out_file,
        out_file_correct,
        column_list, column_types,
        condition_values,
        condition_frequency,
        guaranteed,
    )


if __name__ == '__main__':
    Fire(main)
