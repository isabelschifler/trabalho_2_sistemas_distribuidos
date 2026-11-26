#!/usr/bin/env python3
"""T2 - Cliente REST em Python (somente biblioteca padrao) do servidor central.

Uso:  python3 cliente_t2.py <NomeDoCliente> [host:porta]
      python3 cliente_t2.py ClienteA                  (servidor em localhost:8080)
      python3 cliente_t2.py ClienteB 192.168.0.10:8080

Como a notificacao funciona (SSE, sem polling): ao iniciar, o cliente abre UMA conexao
GET /eventos?cliente=<nome> numa thread propria. O servidor escreve nessa conexao quando
repassa o token. O menu continua livre enquanto isso.
"""
import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime

NOME = ""
BASE = "http://localhost:8080"
trava_tela = threading.Lock()          # evita que o menu e a thread de notificacao misturem as linhas


def log(msg):
    """Imprime uma linha de log com horario."""
    with trava_tela:
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}", flush=True)


def enviar(metodo, caminho, corpo=None):
    """Faz uma requisicao HTTP/JSON. Devolve (status, texto). Erros 4xx/5xx tambem sao respostas validas."""
    dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    cab = {"Content-Type": "application/json; charset=utf-8"} if dados else {}
    rq = urllib.request.Request(BASE + caminho, data=dados, method=metodo, headers=cab)
    log(f"ENVIADO   {metodo} {caminho} {json.dumps(corpo) if corpo else ''}")
    try:
        with urllib.request.urlopen(rq) as r:
            status, texto = r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        status, texto = e.code, e.read().decode("utf-8")
    log(f"RESPOSTA  HTTP {status} {texto}")
    return status, texto


# ----------------------------------------------------------------- notificacoes (thread propria)


def ouvir_eventos(conectado):
    """Mantem o GET /eventos aberto e mostra cada evento assim que chega (SSE)."""
    rq = urllib.request.Request(
        f"{BASE}/eventos?cliente={NOME}",
        headers={"Accept": "text/event-stream"}
    )

    try:
      with urllib.request.urlopen(rq) as r:
            conectado.set()  # confirma somente quando a conexao foi estabelecida

            evento = None

            for bruto in r:
                linha = bruto.decode("utf-8").rstrip("\r\n")

                if linha.startswith("event:"):
                    evento = linha[6:].strip()

                elif linha.startswith("data:") and evento:
                    d = json.loads(linha[5:].strip())

                    log(
                        f"NOTIFICACAO >>> evento '{d['evento']}': "
                        f"o recurso {d['recurso']} agora e SEU. "
                        f"Pode usar a secao critica e depois liberar."
                    )

                    with trava_tela:
                        print("opcao> ", end="", flush=True)

                    evento = None

    except Exception as e:
        log(f"Falha no canal de notificacoes: {e}")


# ----------------------------------------------------------------- operacoes do menu

def pedir(rid):
    status, texto = enviar("POST", f"/recursos/{rid}/acesso", {"cliente": NOME})
    if status == 200:
        print(f"-> Acesso CONCEDIDO: voce esta na secao critica de {rid}.")
    elif status == 202:
        pos = json.loads(texto)["posicao"]
        print(f"-> {rid} ocupado. Voce entrou na fila (posicao {pos}). "
              f"Pode usar o menu: o aviso aparece sozinho quando chegar a sua vez.")
    else:
        print(f"-> Pedido recusado: {texto}")


def liberar(rid):
    status, texto = enviar("DELETE", f"/recursos/{rid}/acesso?cliente={NOME}")
    if status == 200:
        novo = json.loads(texto)["novoDono"]
        print(f"-> {rid} liberado." + (f" Token repassado para {novo}." if novo else " Ninguem na fila: recurso livre."))
    else:
        print(f"-> Liberacao recusada: {texto}")


def estado():
    status, texto = enviar("GET", f"/recursos?cliente={NOME}")
    if status == 200:
        for r in json.loads(texto):
            print(f"   {r['id']}: na SC = {r['dono'] or '(livre)'} | fila = {r['fila'] or '(vazia)'}")


def menu():
    print(f"""
=== {NOME} ===
 1) Pedir R1     2) Pedir R2
 3) Liberar R1   4) Liberar R2
 5) Ver estado dos recursos
 0) Sair""")



def main():
    global NOME, BASE

    if len(sys.argv) < 2:
        print(__doc__)
        return

    NOME = sys.argv[1]

    if len(sys.argv) > 2:
        BASE = "http://" + sys.argv[2]

    conectado = threading.Event()

    threading.Thread(
        target=ouvir_eventos,
        args=(conectado,),
        daemon=True
    ).start()

    # Aguarda a confirmacao da conexao SSE
    if not conectado.wait(timeout=5):
        print("Erro: nao foi possivel conectar ao canal de notificacoes SSE.")
        print("Verifique se o servidor esta funcionando e tente novamente.")
        return

    acoes = {
        "1": lambda: pedir("R1"),
        "2": lambda: pedir("R2"),
        "3": lambda: liberar("R1"),
        "4": lambda: liberar("R2"),
        "5": estado
    }

    while True:
        menu()
        op = input("opcao> ").strip()

        if op == "0":
            break

        acoes.get(op, lambda: print("opcao invalida"))()


if __name__ == "__main__":
    main()

