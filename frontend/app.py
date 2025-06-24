from flask import Flask, render_template
import pandas as pd
import glob

app = Flask(__name__)

@app.route('/')
def index():
    archivos = glob.glob("../datos/pedidos/*.csv")
    df = pd.concat([pd.read_csv(a) for a in archivos]) if archivos else pd.DataFrame()
    pedidos = df.sort_values(by="fecha", ascending=False).to_dict(orient="records")
    return render_template('index.html', pedidos=pedidos)

if __name__ == '__main__':
    app.run(debug=True)
