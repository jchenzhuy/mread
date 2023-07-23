import os
import io
import re
import dateutil
import pandas as pd
from functools import partial


class MSCIIndexFileReader(object):
    """
    template to read index file in msci custom format
    """
    COMMENT_LINE_START = '*'
    DEFINITION_LINE_START = '#'
    EOD_LINE_START = '#EOD'
    LEGACY_LINE_START = 'SS'

    def __init__(self, filepath_or_iobuffer, delimiter='|'):
        if isinstance(filepath_or_iobuffer, str) and os.path.isfile(filepath_or_iobuffer):
            self.file = open(filepath_or_iobuffer)
        elif isinstance(filepath_or_iobuffer, io.IOBase):
            self.file = filepath_or_iobuffer
        else:
            self.file = open(filepath_or_iobuffer)
        self.delimiter = delimiter

    @classmethod
    def is_comment_line(cls, line):
        return line.startswith(cls.COMMENT_LINE_START)

    @classmethod
    def is_definition_line(cls, line):
        return line.startswith(cls.DEFINITION_LINE_START)

    @classmethod
    def is_eod_line(cls, line):
        return line.startswith(cls.EOD_LINE_START)

    @classmethod
    def is_legacy_header(cls, line):
        return line.startswith(cls.LEGACY_LINE_START)

    @staticmethod
    def data_convert_func(field_type, field_width, decimal_len):
        if field_type == 'S':  # string
            return str
        elif field_type == 'N':  # numeric
            if decimal_len > 0 or field_width > 10:
                return float
            else:
                return int
        elif field_type == 'D':  # date
            return partial(dateutil.parser.parse)
        else:
            return str

    def read(self, parse_str=True):
        """
        read MSCI daily index data in custom format, and construct a pandas data frame
        :return: pandas.DataFrame
        """
        with self.file as f:
            reading_meta = False
            done_reading_meta = False
            num_attributes = -1
            attribute_list = list()
            attribute_func = dict()
            data_list = list()
            for line_num, line in enumerate(f):
                if self.is_eod_line(line):
                    break
                if self.is_comment_line(line) or self.is_legacy_header(line):
                    continue
                if self.is_definition_line(line):
                    if done_reading_meta:
                        continue
                    else:
                        tokens = re.split('\s+', line.replace(self.DEFINITION_LINE_START, '').strip())
                        if not reading_meta:
                            num_attributes = int(tokens[0])
                            reading_meta = True
                        else:
                            attribute_list.append(tokens[-4])
                            attribute_func[tokens[-4]] = self.data_convert_func(tokens[-3], int(tokens[-2]), int(tokens[-1]))
                            if len(attribute_func) == num_attributes:
                                done_reading_meta = True
                        continue
                # process content
                if line.startswith(self.delimiter):
                    line = line[1:].strip()
                    tokens = [None if token.strip() == '' else token.strip() for token in line.split(self.delimiter)]
                    if len(tokens) != num_attributes:
                        raise ValueError('expecting {exp} attributes, but getting {act} values: {line}'.format(
                            exp=str(num_attributes), act=str(len(tokens)), line=line))
                    data_list.append(tokens)
            index_data = pd.DataFrame(data=data_list, columns=attribute_list, dtype=str)
            if parse_str:
                for col_name in index_data.columns:
                    index_data[col_name] = index_data[col_name].map(lambda x: x if pd.isnull(x) else attribute_func[col_name](x))
            return index_data
