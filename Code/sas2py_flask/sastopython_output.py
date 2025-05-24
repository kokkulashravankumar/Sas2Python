import os
import sys
import pandas as pd

# Save the original stdout so we can restore it later
original_stdout = sys.stdout

# Open a new file to write the output to
with open('Python_output.txt', 'w') as f:
    # Set sys.stdout to write to the file
    sys.stdout = f
    #os.remove('Python_output.txt')
    # Execute the code in the file
    with open('pythonstatments.txt', 'r') as code_file:
        code = code_file.read()
        exec(code)


    # Flush the output to the file and close it
    f.flush()
    f.close()

# Restore the original stdout
sys.stdout = original_stdout










#
# x = b'proc import out=material_master\r\n'
#
# print(x.decode())



import subprocess

# with open("output.py", "ab") as f:
#     subprocess.check_call(["python", "sas2py_v4.py"], stdout=f)
#
# with open("python_output.txt", "wb") as f:
#     subprocess.check_call(["python", ".py"], stdout=f)


import pandas as pd
# with open('somefile.txt') as sas_input_file:
#     global sas_input
#     sas_input = sas_input_file.read()


# with open('somefile.txt') as sas_input_file:
#     sas_input = sas_input_file.read()
#     exec(sas_input)
#




# import json
# import ast
#
# def execute_and_return_output(code):
#     # Use the built-in `ast` module to parse the code
#     tree = ast.parse(code)
#     # Create an empty dictionary to store the output
#     output = {}
#
#     # Iterate through all of the top-level statements in the code
#     for node in tree.body:# If the statement is an `Expr` node (an expression that returns a value),
#         # then execute it and store the result in the output dictionary
#         if isinstance(node, ast.Expr):
#           output[node] = eval(node)
#
#       # Return the output as a JSON string
#       return json.dumps(output)
#
# # Example usage:
# code = 'material_master = pd.read_csv("./data1.csv")'
# output = execute_and_return_output(code)
# print(output)