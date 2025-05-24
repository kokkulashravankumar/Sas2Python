import pandas as pd
import numpy as np
from pandasql import sqldf
import itertools
import more_itertools as mit
import os
from hurry.filesize import size
from datetime import datetime
import re
from flask import send_file

format_value = dict()
format_picture = dict()


def find_between(s, start, end):
    return (s.split(start))[1].split(end)[0]


def generate_half_columns(list1):
    start_str = list1[0].strip("'")
    new_list = []
    for str_item in list1[1:]:
        half_col = '-'.join([start_str, str_item])
        new_list.append(half_col)
    new_str = ' '.join(new_list)
    return new_str


def get_columns(str_1):
    last_bracket = str_1.rindex('(')
    for str_index in range(last_bracket, len(str_1)):
        if ')' == str_1[str_index]:
            derived_list = str_1[last_bracket + 1:str_index].split(' ')
            new_str = generate_half_columns(derived_list)
            generated_str = str_1[:last_bracket] + new_str + str_1[str_index + 1:]
            return generated_str


def between(startString, endString, myString):
    mySubString = myString[myString.find(startString) + len(startString):myString.find(endString)]
    return mySubString


def format_option_splitting(line):
    new_line = find_between(line, 'format ', ';')
    new_line = new_line.strip()
    new_line = [x.strip() for x in new_line.split('.') if x]
    new_dict = dict()
    for item in new_line:
        key_value = item.split()
        new_dict[key_value[0]] = key_value[1]
    return new_dict


def format_value_dict_generate(line_item):
    global format_value
    key = line_item.split(' ')[1]
    format_value[key] = dict()
    for key_value in line_item.split(' ')[2:]:
        key1 = find_between(' ' + key_value, ' ', '=')
        value1 = find_between(' ' + key_value + ' ', '=', ' ')
        if '-' in key1:
            key1 = tuple(key1.split('-'))
            format_value[key][key1] = value1.strip("'")
        else:
            format_value[key][key1] = value1.strip("'")


def format_picture_dict_generate(line_item):
    global format_picture
    key = line_item.split(' ')[1]
    new_line = find_between(line_item, f'{key} ', ';')
    format_picture[key] = dict()
    new_line_list = new_line.replace('(', '').replace(')', "").split(' ')
    temp_key = None
    proc_format_picture_options = ['PREFIX', 'FILL', 'MULT']
    for each_item in new_line_list:
        key1 = find_between(" " + each_item + " ", ' ', '=')
        if key1.upper() in proc_format_picture_options:
            value1 = find_between(" " + each_item + " ", '=', ' ')
            format_picture[key][temp_key][key1] = value1.strip("'")
        else:
            if '-' in key1:
                key1 = tuple(key1.split('-'))
            format_picture[key][key1] = dict()
            temp_key = key1
            value1 = find_between(' ' + each_item + ' ', '=', ' ')
            format_picture[key][key1]['value'] = value1.strip("'")


def format_value_code_generator(dataframe, format_col, format_var):
    stmt = ''
    func4 = '''def format_func1(x):
    tuple_keys=[key for key in format_value['{0}'].keys() if isinstance(key,tuple)]
    if x in format_value['{1}'].keys():
        return format_value['{7}'][x]
    elif tuple_keys:
        for key_item in tuple_keys:
            if isinstance(x,str):
                if len(str(x)) == 1:
                    if x >= key_item[0][0] and x <= key_item[1][0]:
                        return format_value['{2}'][key_item]
                else:
                    if x[0] >= key_item[0][0] and x[0] < key_item[1][0]:
                        return format_value['{3}'][key_item]
            else:
                if x >= int(key_item[0]) and x <= int(key_item[1]):
                    return format_value['{4}'][key_item]
    if 'other' in format_value['{5}'].keys():
        return format_value['{6}']['other']
    else:
        return x'''
    func4 = func4.format(format_var, format_var, format_var, format_var, format_var, format_var, format_var, format_var)
    stmt += func4 + "\n\n"
    stmt += dataframe + "['" + format_col + "']=" + dataframe + "['" + format_col + "'].apply(format_func1)\n"
    return stmt


def format_picture_code_generator(dataframe, format_col, format_var):
    stmt = ''
    func3 = '''def format_picture_decimal(x,value):
    if x:
        x=[*x][::-1]
        new_str=''
        for each_item in value:
            if each_item.isdigit():
                if x:
                    new_str+=x.pop()
                else:
                    new_str+='0'
            else:
                new_str+=each_item
        return new_str
    else:
        value=re.sub('[1-9]','0',value)
        return value'''
    stmt += func3 + "\n\n"
    func2 = '''def format_picture_string(x,tuple_values):
    value=tuple_values['value'][::-1]
    dot=False
    if 'mult' in tuple_values.keys():
        x=x*int(tuple_values['mult'])
    else:
        if '.' in tuple_values['value']:
            dot=True
            value=tuple_values['value'].split('.')[0][::-1]
            x_decimal_part=''
            if '.' in str(x):
                x_decimal_part=str(x).split('.')[1]
            decimal_string=format_picture_decimal(x_decimal_part,tuple_values['value'].split('.')[1])     
    x_list=[*str(int(x))]
    new_str=''
    if bool(re.search('[0-9]',value)):
        for each_index in range(len(value)):
            if value[each_index].isdigit():
                if x_list:
                    new_str+=x_list.pop()
                else:
                    if int(value[each_index]) == 0:
                        if bool(re.search('[1-9]',value[each_index+1:])):
                            new_str+='0'
                        else:
                            if 'prefix' in tuple_values.keys():
                                if tuple_values['prefix'] not in new_str:
                                    new_str+=tuple_values['prefix']
                                    continue
                                if 'fill' in tuple_values.keys():
                                    new_str+=tuple_values['fill']
                    else:
                        new_str+='0'
            else:
                if x_list and bool(re.search('[0-9]',value[each_index+1:])):
                    new_str+=value[each_index]
                elif not x_list and bool(re.search('[1-9]',value[each_index+1:])):
                    new_str+=value[each_index]
                else:
                    if 'prefix' in tuple_values.keys():
                        if tuple_values['prefix'] not in new_str:
                            new_str+=tuple_values['prefix']
                            continue
                        if 'fill' in tuple_values.keys():
                            new_str+=tuple_values['fill']
        if dot:
            new_str=new_str[::-1]+'.'+decimal_string
            return new_str
        else:
            return new_str[::-1]
    else:
        return tuple_values['value']'''
    stmt += func2 + "\n\n"
    func1 = '''def format_func(x):
    for tuple_item,tuple_item_values in format_picture['{0}'].items():
        if isinstance(tuple_item,tuple):
            if tuple_item[0].lower()=='low':
                small={1}['{2}'].min()
            else:
                small=int(tuple_item[0])
            if tuple_item[1].lower()=='high':
                large={3}['{4}'].max()
            else:
                large=int(tuple_item[1])
            if x>=small and x<=large:
                new_str=format_picture_string(x,tuple_item_values)
                return new_str    
        else:
            new_str=format_picture_string(x,tuple_item_values)
            return new_str                             
    return x'''
    func1 = func1.format(format_var, dataframe, format_col, dataframe, format_col)
    stmt += func1 + "\n"
    stmt += dataframe + "['" + format_col + "']=" + dataframe + "['" + format_col + "'].apply(format_func)\n"
    return stmt


def proc_report(line):
    global sas_input, count
    input_file = find_between(line, 'data=', ' ')
    python_stmt = "dataframe" + " = " + "pd.read_csv(" + input_file + ")"

    all_lines = []
    for line in sas_input[count:]:
        if line.lower().strip() == 'run':
            break
        all_lines.append(line)
    columns = []
    new_columns = []
    extra_columns = []
    grouping_columns = []
    across_columns = []
    agg_dict = {}
    where_dict = {}
    break_column = None
    analysis_dict = {}
    rbreak = False
    aggregations = ['N', 'MEAN', 'SUM']
    for line in all_lines:
        line = line.strip()
        end_char = "," if "," in line else ";"
        if 'columns' in line.lower() and "('" in line:
            str_1 = find_between(line, "columns ", end_char)
            while True:
                if "('" not in str_1:
                    break
                else:
                    str_1 = get_columns(str_1)
                columns = str_1.split(' ')
                columns = [tuple(i.split('-')) for i in columns]
                new_columns = [col[-1] for col in columns]
        if 'columns' in line.lower() and "('" not in line:
            new_columns = find_between(line, "columns ", end_char).split(' ')
        if 'columns' in line.lower() and "," in line:
            extra_columns = find_between(line, ",(", ')').split(' ')
            for col in extra_columns:
                agg_dict[col] = ['sum']
        if 'define' in line.lower():
            agg_type = find_between(line, '/', ' ')
            col = find_between(line, ' ', '/')
            if agg_type.upper() == 'ACROSS':
                across_columns.append(col)
                if not extra_columns:
                    agg_dict[col] = ['count']
            elif agg_type.upper() == 'GROUP':
                grouping_columns.append(col)
            elif agg_type.upper() in aggregations:
                if agg_type.upper() == 'N':
                    agg_dict[col] = ['count']
                else:
                    agg_dict[col] = [agg_type]
        if 'where' in line.lower():
            new_line = find_between(line, "where ", "')") + "')".upper()
            column_key = find_between(new_line, '(', ") ")
            column_value = find_between(new_line, "('", "')").replace("'", "")
            where_dict[column_key] = column_value.split(',')
        if 'rbreak' in line.lower():
            rbreak = True
            continue
        if 'break' in line.lower():
            break_column = find_between(line, 'AFTER ', '/')
        if 'analysis' in line.lower():
            col = find_between(line, "define ", "/")
            agg_operation = find_between(line, "analysis ", ";")
            analysis_dict[col] = agg_operation
    if not across_columns and not grouping_columns:
        if columns:
            python_stmt += "\r\nindexes=pd.MultiIndex.from_tuples(" + str(columns) + ")"
            python_stmt += "\r\ndataframe=dataframe[" + str(new_columns) + "]"
            python_stmt += "\r\ndataframe.columns=indexes"
        elif not columns:
            python_stmt += "\r\ndataframe=dataframe[" + str(new_columns) + "]"

        if rbreak:
            python_stmt += "\r\ndataframe['total']=dataframe.select_dtypes(include=np.number).sum(axis=1)"
            python_stmt += "\r\nprint(dataframe)"
            python_stmt += "\r\nprint(dataframe.select_dtypes(include=np.number).sum())"
        else:
            python_stmt += "\r\nprint(dataframe)"

    if not across_columns and grouping_columns:
        if where_dict:
            where_col = [where_col for where_col in grouping_columns if where_col.upper() in where_dict.keys()]
            if where_col:
                python_stmt += "\r\ndataframe=dataframe[dataframe['" + str(where_col[0]) + "'].str.upper().isin(" + str(
                    where_dict[where_col[0].upper()]) + ")]"
        python_stmt += "\r\n" + "dataframe=dataframe.sort_values(by=" + str(grouping_columns) + ")" + "[" + str(
            new_columns) + "]"
        if rbreak:
            python_stmt += "\r\n" + "dataframe['total']=dataframe.select_dtypes(include=np.number).sum(axis=1)"
            python_stmt += "\r\nprint(dataframe)"
            python_stmt += "\r\nprint(dataframe.select_dtypes(include=np.number).sum())"
        else:
            python_stmt += "\r\nprint(dataframe)"
        if analysis_dict:
            for col, value in analysis_dict.items():
                python_stmt += "\r\n" + col + "_" + value + "=dataframe['" + col + "']." + value + "()"
                python_stmt += "\r\nprint(" + col + "_" + value + ")"
    if across_columns:
        python_stmt += "\t\n" + "out_put=pd.DataFrame()"
        for col in across_columns:
            if where_dict:
                all_columns = grouping_columns + [col]
                where_col = [where_col for where_col in all_columns if where_col.upper() in where_dict.keys()]
                if where_col:
                    python_stmt += "\r\ndataframe=dataframe[dataframe['" + str(
                        where_col[0]) + "'].str.upper().isin(" + str(where_dict[where_col[0].upper()]) + ")]"

            python_stmt += "\r\n" + "new_df=dataframe.groupby(" + str(grouping_columns + [col]) + ").agg(" + str(
                agg_dict) + ")"
            python_stmt += "\r\n" + "out_put=pd.concat([out_put,new_df])"
            python_stmt += "\r\ndataframe=out_put.copy()"
            python_stmt += "\r\nprint(dataframe)"
            if break_column:
                python_stmt += "\r\ndataframe['total']=dataframe.select_dtypes(include=np.number).sum(axis=1)"
                python_stmt += "\r\nprint(dataframe)"
                python_stmt += "\r\nfinal_sum=dataframe.groupby(['" + break_column + "']).sum()"
                python_stmt += "\r\nprint(final_sum)"
            if rbreak:
                python_stmt += "\r\nprint(dataframe.select_dtypes(include=np.number).sum())"

    return python_stmt


def proc_sql(line):
    global sas_input, count, export_dataframe, export_file
    line = sas_input[count + 1]

    title_key = 'TITLE'
    if line.find(title_key.lower()) != -1:
        line = sas_input[count + 2]

    tup = line.lower().partition("select")[2]
    line2 = ''.join(tup)
    # print(tup, line2)
    return export_dataframe + ' = pd.read_sql("select ' + line2 + '", connect) \r\n' + export_dataframe + ".to_excel(" + export_file + ",index = False)"

    return line


def proc_export(line):
    dataframe = find_between(line, 'data=', ' ')
    output_file = find_between(line, 'outfile=', ' ')
    python_stmt = dataframe + ".to_csv(" + output_file + ",index = False)"
    return python_stmt


def proc_import(line):
    input_file = find_between(line, 'datafile=', ' ')
    dataframe = find_between(line, 'out=', ' ')
    python_stmt = dataframe + " = " + "pd.read_csv(" + input_file + ")"
    return python_stmt


def proc_append(line):
    input_file = find_between(line, 'base=', ' ')
    dataframe = find_between(line, 'data=', ';')
    python_stmt = input_file + " = " + "pd.concat([" + input_file + "," + dataframe + "])"
    return python_stmt


def proc_print(line):
    global sas_input, count
    python_stmt = ''
    dataframe = find_between(line, 'data=', ';')
    if sas_input[count].strip().split(' ')[0].lower() == 'format':
        format_dict = format_option_splitting(sas_input[count].strip())
        # python_stmt+='format_dict='+str(format_dict)+'\n'
        for format_col, format_var in format_dict.items():
            if format_var in format_value.keys():
                python_stmt += format_value_code_generator(dataframe, format_col, format_var)
            elif format_var in format_picture.keys():
                python_stmt += format_picture_code_generator(dataframe, format_col, format_var)
    python_stmt += "print(" + dataframe + ")"
    return python_stmt


def proc_sort(line):
    global sas_input, count

    dataframe = find_between(line, 'data=', ' ;')
    line1 = sas_input[count]
    if ' ' in line1:
        variable_to_sort = find_between(line1, 'by ', ';')
        python_stmt = dataframe + ".sort_values(['" + variable_to_sort + "'], ascending=[True], inplace=True)"
        if 'descending' in line1:
            variable_to_ascend = find_between(line1, 'by ', ' descending')
            variable_to_descend = find_between(line1, 'descending ', ';')
            var_asc_list = [variable_to_ascend]
            asc_list = [True]
            if ' ' in variable_to_descend:
                variable_to_descend = variable_to_descend.split()
                var_asc_list.extend(variable_to_descend)
                false_list = [False] * len(variable_to_descend)
                asc_list.extend(false_list)
                python_stmt = dataframe + ".sort_values(" + str(var_asc_list) + ", ascending=" + str(
                    asc_list) + ", inplace=True)"
            else:
                python_stmt = dataframe + ".sort_values(['" + str(
                    variable_to_ascend) + "','" + variable_to_descend + "'], ascending=[True, False], inplace=True)"
        else:
            variable_to_sort = find_between(line1, 'by ', ';').split(' ')
            var_list = []
            ascending_list = [True]
            for var in variable_to_sort:
                var_list.append(var)
                python_stmt = dataframe + ".sort_values(" + str(var_list) + ", ascending=" + str(
                    ascending_list * len(var_list)) + ", inplace=True)"

    if 'nodupkey' in line:
        dataframe = find_between(line, 'data=', ' nodupkey')
        dup_variable = find_between(line1, 'by ', ' ;')
        python_stmt = f'{dataframe} = {dataframe}' + ".drop_duplicates('" + dup_variable + "', keep='first').sort_values('" + dup_variable + "', ascending=True)"

    if 'keep' and 'nodupkey' and 'out' in line:
        dataframe = find_between(line, 'data=', ' ')
        keep_var = find_between(sas_input[count - 1], '(keep=', ')')
        out_df = find_between(line, 'out=', ';')
        python_stmt1 = f'{dataframe} = {dataframe}'"[['" + keep_var + "']].drop_duplicates('" + keep_var + "', keep='first').sort_values('" + keep_var + "', ascending=True).copy()"
        python_stmt2 = f'{out_df} = {dataframe}.' + "to_csv('./" + out_df + ".csv', index=False)"
        return f'{python_stmt1}\n{python_stmt2}'

    return python_stmt


def proc_freq(line):
    global sas_input, count
    dataframe = find_between(line, 'data=', ';')
    line = sas_input[count]
    variable_to_find_freq = find_between(line, 'tables', ';').strip()
    python_stmt = f"{dataframe} = {dataframe}['{variable_to_find_freq}'].value_counts().sort_index()\n"
    if '/' in line:
        missing_values = find_between(line, '/', ';').strip()
        variable_to_find_freq = find_between(line, 'tables', '/').strip()
        if missing_values:
            python_stmt = f"{dataframe} = {dataframe}['{variable_to_find_freq}'].value_counts(dropna=False).sort_index()\n"

    python_stmt += """frequency_dataframe = pd.DataFrame({{'{variable_column}': {dataframe}.index,'Frequency': {dataframe}.values,'Percent': (({dataframe}.values / {dataframe}.values.sum()) * 100).round(2),'Cumulative Frequency': {dataframe}.values.cumsum(),
        'Cumulative Percent': (({dataframe}.values.cumsum() / {dataframe}.values.sum()) * 100).round(2)}})""".format(
        variable_column=variable_to_find_freq, dataframe=dataframe)
    return python_stmt


def proc_means(line):
    global sas_input, count
    dataframe = find_between(line, 'data=', ';')
    line = sas_input[count]
    x = find_between(line, 'VAR', ';').strip()
    output = f"output_{dataframe} = {dataframe}['{x}'].describe()"
    return output


def proc_transpose(line):
    global sas_input, count
    dataframe = find_between(line, 'data=', ' out=transposed_material_master')
    output = find_between(line, 'out=', ';')
    return f"{output} = {dataframe}.transpose()"


def proc_compare(line):
    global sas_input, count
    # PROC COMPARE BASE = material_master , COMPARE = transposed_material_master;
    dataframe1 = find_between(line, ' base=', ', compare')
    dataframe2 = find_between(line, 'compare=', ' ')
    comapre = f"compare = datacompy.Compare({dataframe1},{dataframe2},on_index=True)"
    # compare = datacopy.Compare(material_master ,transposed_material_master,on_index=True)
    return comapre


def libname(line):
    global export_dataframe, export_file
    export_dataframe = line.split()[1]
    export_file = line.split()[3].replace(';', '')
    return 0


def proc_contents(line):
    '''
    This function read the dataframe and show the information of the dataframe
    dataframe.info
    :param line: input from .txt file(sascode)
    :return: The information contains the number of columns, column labels, column data types, memory usage, range index, and the number of cells in each column (non-null values
    '''
    data_value = find_between(line, "data=", " ")
    output_value = find_between(line, "out=", " ")[1:-1]
    dataframe = "pd.read_csv(" + data_value + ")"
    python_stmt = f"{output_value} = {dataframe}.info()"
    return python_stmt


def proc_datasets(line):
    '''
    This function take the path as input from libname function (export_file) and return the folder detials
    :param line: Take the input from libname function export_file
    :return: display information(name,type,filesize,lastmodified) of the files inside  folder
    '''
    path = export_file.replace("'", "")
    directories_dict = {
        'name': [],
        'type': [],
        'file size': [],
        'last modified': []
    }
    for file_info in os.scandir(path):
        file = file_info.name.split('.')
        file_name = file[0]
        if len(file) == 1:
            file_type = 'FOLDER'
        else:
            file_type = file[1].upper()
        file_size = size(file_info.stat().st_size)
        b = datetime.fromtimestamp(file_info.stat().st_mtime)
        modified_time = f'{b.day}/{b.month}/{b.year} {b.hour}:{b.minute}:{b.second}'
        directories_dict['name'].append(file_name)
        directories_dict['type'].append(file_type)
        directories_dict['file size'].append(file_size)
        directories_dict['last modified'].append(modified_time)
    python_stmt = f"dataframe = pd.DataFrame({directories_dict})"
    return python_stmt


def proc_summary(line):
    line = line
    global sas_input, count
    line1 = sas_input[count]
    if "class" not in line1:
        data_input = find_between(line, "data=", " ")
        line = sas_input[count]
        columns = [find_between(line, "var ", ";").split()]
        line = sas_input[count + 1]
        output_dataframe = find_between(line, 'out=', ' ')
        dataframe = "pd.read_csv(" + data_input + ")"
        python_stmt = f'''{output_dataframe} = {dataframe}{columns}.describe().loc[["count", "min", "max","mean","std"]]'''
    else:
        data_input = find_between(line, "data=", " ")
        line = sas_input[count]
        columns = find_between(line, "class ", ";").split()
        line = sas_input[count + 2]
        output_dataframe = find_between(line, 'out=', ' ')
        dataframe = "pd.read_csv(" + data_input + ")"
        python_stmt = f'''{output_dataframe}={dataframe}.groupby({columns}).agg(["count", "min", "max","mean","std"])'''
    return python_stmt


def proc_rank(line):
    global count, sas_input
    # .replace
    line = line.replace("=", "").replace("'", " ")
    if 'ties' not in line.split():
        input_file = find_between(line, 'data ', '  out')
        dataframe = line.split()[-1]
        line = sas_input[count]
        ranks_value = line.split()[-1]
        line = sas_input[count + 1]
        ranks_variable = line.split()[-1]
        data_frame = f"{dataframe} = pd.read_csv('{input_file}')"
        python_stmt = f'''{dataframe}["{ranks_variable}"] = {dataframe}['{ranks_value}'].rank()'''
    else:
        input_file = find_between(line, 'data ', ' out')
        dataframe = find_between(line, 'out ', '  ties').replace("'", "")
        line1 = sas_input[count]
        ranks_value = line1.split()[-1]
        line2 = sas_input[count + 1]
        ranks_variable = line2.split()[-1]
        ties = line.split()[-1]
        if 'by' in sas_input[count + 2].lower():
            group_value = sas_input[count + 2]
            data_frame = f"{dataframe} = pd.read_csv('{input_file}')"
            python_stmt = f'''{dataframe}["{ranks_variable}"] = {dataframe}.groupby('{group_value.split()[1]}')['{ranks_value}'].rank(method='{ties}')'''
            return f'{data_frame}\n{python_stmt}'
        data_frame = f"{dataframe} = pd.read_csv('{input_file}')"
        python_stmt = f'''{dataframe}["{ranks_variable}"] = {dataframe}['{ranks_value}'].rank(method='{ties}')'''
    return f'{data_frame}\n{python_stmt}'


def proc_tabluate(line):
    global count, sas_input
    input_file = find_between(line, 'data=', ' ')
    python_stmt = "dataframe" + " = " + "pd.read_csv(" + input_file + ")"
    t_line2 = sas_input[count + 1]
    if "," in t_line2 and "all" not in t_line2 and "var" not in t_line2:
        line2 = sas_input[count + 1].split()[1]
        line3 = line2.split(",")
        index_value = line3[0]
        cloums_values = line3[1]
        python_stmt1 = f"dataframe = dataframe.pivot_table(index = ['{index_value}'],columns= ['{cloums_values}'],aggfunc = 'count')"
        return f'{python_stmt}\n{python_stmt1}'
    elif "*" in t_line2:
        line2 = sas_input[count + 1].split()[1]
        line4 = line2.split("*")
        python_stmt1 = f"dataframe = dataframe.pivot_table(columns={line4}, aggfunc='count')"
        return f'{python_stmt}\n{python_stmt1}'
    elif "all" in t_line2:
        line2 = sas_input[count + 1].split(',')
        line3 = line2[0]
        index_value = find_between(line3, "tables ", ' all=')
        total_value = find_between(line3, 'all=', ' ').replace("'", "")
        python_stmt1 = f"dataframe = dataframe.pivot_table(index=['{index_value}'],columns= ['{line2[1]}'],margins=True,margins_name='{total_value}')"
        return f'{python_stmt}\n{python_stmt1}'
    elif "var" in t_line2.lower():
        var_value = t_line2.replace('var', '').replace(' ', '').split(',')
        line2 = sas_input[count + 2]
        line3 = line2.split(",")
        index_values = line3[0].replace('tables', '').split('*')[0].replace(" ", "")
        columns_values = line3[1].split('*')[0]
        line_agg = find_between(line2, '(', ')')
        agg_values = line_agg.split()[1:]
        if "all" in line2.lower():
            total_value = find_between(line2, 'all=', ',')
            python_stmt1 = f"dataframe = dataframe.pivot_table(values= {var_value},index = ['{index_values}'],columns= ['{columns_values}'],margins=True,margins_name={total_value},aggfunc={agg_values})"
            return f'{python_stmt}\n{python_stmt1}'
        if len(line_agg.split()) == 1:
            python_stmt1 = f"dataframe = dataframe.pivot_table(values= {var_value},index = ['{index_values}'],columns= ['{columns_values}'],aggfunc={line_agg.split()})"
            return f'{python_stmt}\n{python_stmt1}'
        python_stmt1 = f"dataframe = dataframe.pivot_table(values= {var_value},index = ['{index_values}'],columns= ['{columns_values}'],aggfunc={agg_values})"
        return f'{python_stmt}\n{python_stmt1}'
    elif "all" not in t_line2:
        line2 = sas_input[count + 1].split()[1]
        line3 = line2.split(",")
        python_stmt1 = f"dataframe = dataframe.pivot_table(columns='{line3[0]}', aggfunc='count')"
        return f'{python_stmt}\n{python_stmt1}'


def proc_reg(line):
    global count, sas_input
    line2 = sas_input[count]
    input_file = find_between(line, 'data=', ' ')
    python_stmt = "dataframe" + " = " + "pd.read_csv(" + input_file + ")"
    line = find_between(line2, 'model ', ' ')
    model = line.replace("=", "~")
    results = f"ols('{model}', data=dataframe).fit()"
    python_stmt1 = f'dataframe = {results}.summary()'
    return f'{python_stmt}\n{python_stmt1}'


def proc_univariate(line):
    global count, sas_input
    sas_code = line + ";"
    temp = count
    while sas_input[temp].strip() != "run":
        sas_code += sas_input[temp].strip() + ";"
        temp += 1
    input_file = find_between(sas_code, 'data=', ';')
    columns = find_between(sas_code, ';var ', ';').split()
    if ";class" in sas_code:
        class_group = find_between(sas_code, ';class ', ';').split()
    else:
        class_group = []

    python_stmt = f"""
dataframe =  pd.read_csv({input_file})
moments_cols = pd.MultiIndex.from_product([["MOMENTS"],["Parameter","Result"]])
Moments_df = pd.DataFrame(columns = moments_cols)
Moments_df[('MOMENTS', 'Parameter')] = ["N","Sum Weights","Mean","Sum Observations","Std Deviation",
                                        "Variance","Skewness","Kurtosis","Uncorrected SS",
                                        "Corrected SS","Coeff Variation","Std Error Mean"]

bsm_cols = pd.MultiIndex.from_product([["BASIC STATISTICAL MEASURES"],["Location","Variability"],
                                       ["Parameter","Result"]])
BSM_df = pd.DataFrame(columns = bsm_cols)
BSM_df[('BASIC STATISTICAL MEASURES','Location','Parameter')]=["Mean","Median","Mode",""]
BSM_df[('BASIC STATISTICAL MEASURES','Variability','Parameter')]=["Std Deviation","Variance",
                                                                    "Range","Interquartile Range"]
BSM_df.loc[3,('BASIC STATISTICAL MEASURES','Location','Result')]=""

quantile_cols = pd.MultiIndex.from_product([["QUANTILES"],["Quantile","Estimate"]])
Quantiles_df = pd.DataFrame(columns = quantile_cols)
Quantiles_df[('QUANTILES', 'Quantile')] = ["100% Max","99%","95%","90%","75% Q3","50% Medain",
    "25% Q1","10%","5%","1%","0% Min"]

tests_cols = pd.MultiIndex.from_product([["TESTS FOR LOCATION: Mu0=0"],["Statistic","p Value"],
                                         ["Parameter","Result"]])
Tests_df = pd.DataFrame(columns = tests_cols,index = [
    "Student's t",
    "Sign",
    "Signed Rank"    
])
Tests_df.index.name = "Test"
Tests_df[('TESTS FOR LOCATION: Mu0=0','p Value', 'Parameter')]= ["Pr > |t|","Pr >= |M|","Pr >= |S|"]
Tests_df[('TESTS FOR LOCATION: Mu0=0', 'Statistic', 'Parameter')]= ["t","M","S"]

extreme_cols = pd.MultiIndex.from_product([["EXTREME OBSERAVATIONS"],["Lowest","Highest"],["Value","Obs"]])

def univariate(df):
    moments_res = [
    len(df) , len(df) , df.mean() , df.sum() ,
    df.std() , df.var() , df.skew(), df.kurt() ,
    np.sum(np.square(df)) ,sum([(df.mean()-num )** 2 for num in df]) ,
    (df.std()/df.mean())*100 ,df.sem() ]

    bsm_res = [
        ( df.mean() ),( df.median() ),( df.mode()[0] ),
        ( df.std() ),( df.var() ),( max(df)-min(df) ),
        ( np.percentile(df,75)-np.percentile(df,25) )]

    quantiles_res = []


    quantiles_res = [ num for num in np.quantile(df,[1,0.99,0.95,0.90,0.75,0.50,0.25,0.10,0.05,0.01,0],
    method="averaged_inverted_cdf")]

    t_stat, t_pvalue = stats.ttest_1samp(df,0)
    m_stat, m_pvalue = sign_test(df, mu0=0)
    s_stat, s_pvalue = stats.wilcoxon(df)

    if len(df) >= 5:
        length = 5
    else :
        length = len(df)

    extreme_val_res = list(df.nsmallest(length)) + list(df.nlargest(length))[::-1]
    extreme_obs_res = list(df.nsmallest(length).index) + list(df.nlargest(length).index)[::-1]

    sem = [num for num in range(length)]
    Extreme_obs_df = pd.DataFrame(columns = extreme_cols,index =sem)

    Moments_df[('MOMENTS','Result')] = moments_res

    BSM_df[('BASIC STATISTICAL MEASURES','Location','Result')] = bsm_res[:3] + [""]
    BSM_df[('BASIC STATISTICAL MEASURES', 'Variability','Result')] = bsm_res[3:]

    Quantiles_df[('QUANTILES', 'Estimate')] =quantiles_res 

    Extreme_obs_df[('EXTREME OBSERAVATIONS','Lowest', 'Value')]=extreme_val_res[:length]
    Extreme_obs_df[('EXTREME OBSERAVATIONS', 'Highest', 'Value')]=extreme_val_res[length:]

    Extreme_obs_df[('EXTREME OBSERAVATIONS','Lowest','Obs')]=extreme_obs_res[:length]
    Extreme_obs_df[('EXTREME OBSERAVATIONS', 'Highest','Obs')]=extreme_obs_res[length:]

    Tests_df[('TESTS FOR LOCATION: Mu0=0', 'Statistic','Result')]=[ t_stat,
         m_stat, s_stat]
    Tests_df[('TESTS FOR LOCATION: Mu0=0','p Value','Result')]=[ t_pvalue,
        m_pvalue, s_pvalue]

    print(Moments_df.round(4))
    print(BSM_df.round(4) )
    print(Quantiles_df)
    print(Tests_df.round(4))
    print(Extreme_obs_df) """

    if class_group != []:
        python_stmt += f"""
data=dataframe.groupby({class_group})
for col in {columns}:
    for group in data.groups.keys():
        head = "*****"
        for value in group:
            head += " "+value+" " 
        print(head+" "+col+" *****")
        df = data.get_group(group)[col]
        univariate(df)"""
    else:
        python_stmt += f"""
data = dataframe
for col in {columns}:
  print("*****"+col+"*****")
  current_df = data[col]
  univariate(current_df)
    """
    return python_stmt


def proc_format(line):
    global sas_input, count
    global format_value, format_picture
    all_lines = []
    for line in sas_input[count:]:
        if line.lower().strip() == 'run':
            break
        all_lines.append(line)
    python_stmt = ''
    for line_item in all_lines:
        line_item = line_item.strip()
        line_item = re.sub(' +', ' ', line_item)
        # print(line_item)
        if 'value' == line_item.split(' ')[0].lower():
            format_value_dict_generate(line_item)
            python_stmt += 'format_value=' + str(format_value)
        if 'picture' == line_item.split(' ')[0].lower():
            format_picture_dict_generate(line_item)
            python_stmt += 'format_picture=' + str(format_picture)

    return python_stmt


def data(line):
    global count, sas_input
    sas_code = line + ";"
    temp = count
    while sas_input[temp].strip() != "run":
        sas_code += sas_input[temp].strip() + ";"
        temp += 1

    def split(list_a, chunk_size):
        for i in range(0, len(list_a), chunk_size):
            yield list_a[i:i + chunk_size]

    def keys_extract(sas_code):
        data_var = between("data ", ";", sas_code)
        data_var = set_var = merge_var = update_var = modify_var = by_var = None
        if "data" in sas_code:
            data_var = find_between(sas_code, "data ", ";")
        if "set" in sas_code:
            set_var = find_between(sas_code, "set ", ";").split()
        if "merge" in sas_code:
            merge_var = find_between(sas_code, "merge ", ";").split()
        if "update" in sas_code:
            update_var = find_between(sas_code, "update ", ";").split()
        if "modify" in sas_code:
            modify_var = find_between(sas_code, "modify ", ";").split()
        if "by" in sas_code:
            by_var = find_between(sas_code, "by ", ";").split()
        return data_var, set_var, merge_var, update_var, modify_var, by_var

    data_var, set_var, merge_var, update_var, modify_var, by_var = keys_extract(sas_code)
    input_cols = data_lines_data = []

    if data_var:
        if "input" in sas_code and "datalines" in sas_code:
            input_cols = data_lines_data = []
            for sub_code in re.findall("input.+?;", sas_code):
                input_cols += re.sub(r'[^aA-zZ]', ' ', find_between(sub_code, "input ", ";")).split()
            if "delimiter" in sas_code:
                delimit_list = [item.split(",") for item in find_between(sas_code, "datalines;", ";").split()]
                flat_list = [item for nes_item in delimit_list for item in nes_item]
                data_lines_data = list(split(flat_list, len(input_cols)))
            else:
                data_lines_data = list(split(find_between(sas_code, "datalines;", ";").split(), len(input_cols)))
            return f"""{data_var} = pd.DataFrame({data_lines_data}, columns={input_cols})"""
        if set_var:
            if by_var:
                return f"""{data_var} = pd.concat({set_var}, axis=0, sort=True) """
            return f"""{data_var} = pd.concat({set_var}, axis=0) """
        if merge_var:
            if by_var:
                return f""" {data_var} = pd.merge({merge_var[0]},{merge_var[1]}, on={by_var}) """
            return f""" {data_var} = pd.merge({merge_var[0]},{merge_var[1]}) """
        if update_var:
            if by_var:
                return f""" {data_var} = {update_var[0]}.update({update_var[1]}.groupby({by_var})) """
            return None
        if modify_var:
            if by_var:
                return f""" {data_var} = {modify_var[0]}.modify({modify_var[1]}.groupby({by_var})) """
            return None


def sas_function11(request):
    f12 = request.files['text_file']
    procs = ['PROC SQL', 'PROC SORT', 'PROC APPEND', 'PROC DATASETS', 'PROC DELETE',
             'PROC EXPORT', 'PROC PRINT', 'PROC FORMAT', 'PROC REPORT', 'PROC TABULATE',
             'PROC PRINTTO', 'PROC IMPORT', 'PROC CONTENTS', 'PROC COMPARE', 'LIBNAME',
             'PROC FREQ', 'PROC TRANSPOSE', 'PROC MEANS', 'PROC SUMMARY', 'PROC RANK', 'PROC TABLUATE',
             'PROC REG', 'PROC UNIVARIATE']

    data_steps = ['DATA']
    #with open(file) as sas_input_file:
    global sas_input
    sas_input = f12.read().decode()
    spl = sas_input.split(';')
    sas_input = [s.replace('\r', ' ').replace('\n', ' ').replace('\t', '').replace(' = ', '=') for s in spl]
    global count
    count = 0
    for line in sas_input:
        count = count + 1
        line = line.strip()
        for proc_key in procs:
            line = line.lower()
            if line.find(proc_key.lower()) != -1:
                try:
                    sas_function = proc_key.lower().split()[:2]
                    func = globals()[sas_function[0] + "_" + sas_function[1].replace(';', '')]
                except IndexError:
                    sas_function = proc_key.lower()
                    func = globals()[sas_function.replace(';', '')]
                python_stmt = func(line)
                if python_stmt != 0:
                    with open('pythonstatments.txt', 'a') as the_file:
                        the_file.write(f'{python_stmt}\n')
                    break
