import os

from flask import Flask, request, render_template
from sas_functions import sas_function11
app = Flask(__name__)

@app.route('/')
def home():
   return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    sas_function11(request)
    # text_file = request.files['text_file']
    # contents = text_file.read()
    # from script import main
    # main(input = contents.decode())
    with open("pythonstatments.txt","r") as f:
        contents = f.read()
        lines = contents.split("\n")
    return render_template('view.html', lines=lines)

@app.route('/excutecode', methods=['GET'])
def excutecode2():
    #return  {"hai":"kshravankumar"}
    with open("pythonstatments.txt", "r") as f:
        contents = f.read()
        lines = contents.split("\n")
    return render_template('view.html', lines=lines)





if __name__ == "__main__":
  HOST='0.0.0.0'
  app.run(debug=True,host=HOST)
