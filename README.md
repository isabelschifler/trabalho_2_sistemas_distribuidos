# T2 — Exclusão mútua com servidor central sobre Web Services

**Integrantes:** Isabel Schifler e Suzana Silveira
**Servidor:** Java (`ServidorCentral.java`, só JDK, sem dependências)
**Clientes:** Python 3 (`cliente_t2.py`, só biblioteca padrão)
**Web Service:** REST (HTTP + JSON)
**Notificação:** SSE (Server-Sent Events), sem polling

## Como executar

Servidor (JDK 11+; a porta é opcional, padrão 8080):

```
javac ServidorCentral.java && java ServidorCentral 8080
# ou, sem compilar:  java ServidorCentral.java 8080
```

Clientes (um terminal para cada; o segundo argumento é `host:porta` do servidor e permite rodar em outra máquina):

```
python3 cliente_t2.py ClienteA localhost:8080
python3 cliente_t2.py ClienteB localhost:8080
python3 cliente_t2.py ClienteC localhost:8080
```

Menu: `1`/`2` pedem R1/R2, `3`/`4` liberam R1/R2, `5` mostra o estado, `0` sai.

## Contrato da API

Todos os corpos são JSON em UTF-8. O servidor cria R1 e R2 ao iniciar.

| Verbo | Rota | Corpo / parâmetro | Respostas |
|---|---|---|---|
| GET | `/recursos` | — | `200` lista `[{"id","dono","fila":[...]}]` |
| GET | `/recursos/{id}` | — | `200` estado do recurso · `404` recurso inexistente |
| POST | `/recursos/{id}/acesso` | `{"cliente":"ClienteA"}` | `200` `{"status":"concedido",...}` (recurso livre, ou o cliente já é o dono) · `202` `{"status":"espera","posicao":n}` (entrou na fila, ou já estava) · `400` corpo malformado · `404` recurso inexistente · `409` política anti-impasse · `412` cliente sem canal `/eventos` aberto |
| DELETE | `/recursos/{id}/acesso?cliente=ClienteA` | — | `200` `{"liberado":true,"novoDono":"ClienteB"|null}` · `400` · `403` quem pede não é o dono · `404` |
| GET | `/eventos?cliente=ClienteA` | — | `200` `text/event-stream`, conexão fica aberta. Evento: `event: concedido` / `data: {"recurso":"R1","evento":"concedido","cliente":"ClienteA"}` · `400` |

Observações:

- O **pedido que não pôde ser atendido** (`202`) é o registro de interesse do cliente (subscriber). O servidor (publisher) guarda esse interesse na fila do recurso.
- A notificação **é o próprio token**: quando o dono libera, o servidor tira o primeiro da fila, já o torna dono e só depois o avisa. Se o notificado pedir o recurso de novo, recebe `200 concedido` e não perde a vez.
- Pedido repetido de quem já está na fila devolve `202` com a posição atual e **não duplica** a entrada.
- Liberar um recurso livre também dá `403`, porque quem pede não é o dono.
- Para entrar na fila, o cliente precisa estar com o `/eventos` aberto (o cliente Python abre ao iniciar). Sem isso não haveria como avisá-lo depois.

## Política contra impasse

**Um recurso por cliente de cada vez.** O servidor recusa (`409`) o pedido de um cliente que já detém ou já espera outro recurso. Isso elimina a condição de "segura e espera" (um dos quatro requisitos de Coffman para haver impasse): ninguém segura R1 enquanto espera R2, então o ciclo A→R2→B→R1→A não consegue se formar. A verificação roda dentro do mesmo lock que protege dono e filas.

Custo: um cliente não consegue usar R1 e R2 juntos. O enunciado aceita isso explicitamente. R1 e R2 continuam podendo estar em uso ao mesmo tempo por clientes diferentes.

## Concorrência

- `HttpServer` com `newCachedThreadPool()`: uma thread por requisição. Sem isso, uma conexão SSE aberta bloquearia todas as outras.
- Um único `LOCK` protege donos, filas e o mapa de canais SSE. Toda leitura e alteração acontece dentro de `synchronized (LOCK)`.
- A notificação é enviada **depois** de soltar o lock, e por outra thread (`ExecutorService`). Um cliente lento não trava as demais requisições, e o servidor não espera o cliente notificado.
- No cliente, a notificação chega por uma thread própria, e o menu fica livre enquanto o cliente espera.

## O que foi reaproveitado

Da Atividade 10: a estrutura do servidor com `com.sun.net.httpserver` e `setExecutor`, o SSE com `text/event-stream` + `flush`, o formato de erro `{"erro": ...}`, as funções `campo`/`consulta`, e no cliente Python o `enviar` (urllib) e a leitura de SSE por linhas. Foram reescritos: a fila FIFO, o repasse de token, a notificação "concedido" (a da atividade era só "liberado"), a política anti-impasse e o menu.

## Roteiro de teste com curl (ferramenta genérica)

```
curl -N "http://localhost:8080/eventos?cliente=ClienteB" &        # canal de B (SSE)
curl -i -X POST localhost:8080/recursos/R1/acesso -H 'Content-Type: application/json' -d '{"cliente":"ClienteA"}'   # 200
curl -i -X POST localhost:8080/recursos/R1/acesso -H 'Content-Type: application/json' -d '{"cliente":"ClienteB"}'   # 202 posição 1
curl -i -X DELETE "localhost:8080/recursos/R1/acesso?cliente=ClienteB"   # 403 (não é o dono)
curl -i -X DELETE "localhost:8080/recursos/R1/acesso?cliente=ClienteA"   # 200; o curl -N de B imprime o evento
curl -i localhost:8080/recursos/R9                                         # 404
```

## Perguntas da defesa (seção 4 do enunciado)

**Por que a ordem de atendimento não é a ordem happened-before?** O servidor atende pela ordem em que os pedidos *chegam* a ele, e a chegada depende do atraso da rede. Exemplo: A pede R1 e depois manda uma mensagem a B; B, ao receber, pede R1. Logo "pedido de A → pedido de B" (happened-before). Mas o pedido de B pode chegar ao servidor antes do de A (caminhos com atrasos diferentes), e B entra na frente. No T1, Ricart e Agrawala usam relógios de Lamport, que respeitam essa ordem causal.

**Por que o servidor é ponto único de falha?** Se o coordenador cair, ninguém mais consegue entrar em nenhuma seção crítica, e o estado (donos e filas) existe só nele. Também se o cliente que detém o token cair, o token se perde e a fila trava. (Neste trabalho não há falhas, mas o algoritmo não tolera nenhuma delas.)

**Por que é gargalo?** Todo pedido, toda liberação e toda notificação passam por ele. Com muitos clientes, o desempenho fica limitado pelo servidor, e a carga não se distribui como no algoritmo entre pares.

**Quantas mensagens por entrada e saída da SC?** No algoritmo clássico (Coulouris): **3 mensagens** por ciclo, sendo 2 para entrar (pedido + concessão) e 1 para sair (liberação). Na nossa implementação REST, contando também as respostas HTTP: recurso livre = pedido + resposta `200` (2) e liberação + resposta (2); com espera = pedido + `202` + notificação SSE (a concessão) e liberação + resposta. O núcleo continua sendo pedido, concessão e liberação.

**Segurança, subsistência e ordenação?** Segurança: só há um `dono` por recurso, alterado sob o lock. Subsistência: a fila é FIFO, então todo pedido é atendido (sem inanição), e a política de um recurso por cliente evita impasse. Ordenação: FIFO **por ordem de chegada ao servidor**, e não por happened-before (ver acima).
