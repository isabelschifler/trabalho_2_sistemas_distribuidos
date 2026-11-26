import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * T2 - Exclusao mutua com SERVIDOR CENTRAL sobre Web Services (REST + SSE).
 * Servidor em Java (somente JDK 11+, sem dependencias). Clientes em Python (cliente_t2.py).
 *
 * Contrato (detalhes no README.md):
 *   GET    /recursos                      estado de R1 e R2 (dono + fila)
 *   GET    /recursos/{id}                 estado de um recurso
 *   POST   /recursos/{id}/acesso          pedir acesso   corpo {"cliente":"ClienteA"}
 *   DELETE /recursos/{id}/acesso?cliente= liberar (devolver o token)
 *   GET    /eventos?cliente=X             SSE: o servidor publica "concedido" para o cliente X
 *
 * Politica contra impasse: UM RECURSO POR CLIENTE DE CADA VEZ (elimina "segura e espera").
 */
public class ServidorCentral {

    /** Estado de um recurso. So pode ser lido/alterado dentro de synchronized (LOCK). */
    static class Recurso {
        final String id;
        String dono;                                        // null = livre
        final LinkedList<String> fila = new LinkedList<>(); // FIFO de pedidos em espera
        Recurso(String id) { this.id = id; }
    }

    /** Resposta HTTP pronta (status + corpo JSON). */
    static class Resp {
        final int status; final String corpo;
        Resp(int status, String corpo) { this.status = status; this.corpo = corpo; }
    }

    static final Object LOCK = new Object();                       // UM lock protege dono, filas e fluxos
    static final Map<String, Recurso> recursos = new LinkedHashMap<>();
    static final Map<String, OutputStream> fluxos = new HashMap<>(); // cliente -> conexao SSE aberta
    static final ExecutorService notificador = Executors.newCachedThreadPool(); // envia notificacoes fora do lock
    static final Pattern NOME = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    // JSON plano estrito: {"chave": valor, ...} com valor string, numero, true, false ou null. Rejeita chave/aspas faltando e lixo extra.
    static final String JSON_VALOR = "(?:\"[^\"\\\\]*\"|-?\\d+(?:\\.\\d+)?|true|false|null)";
    static final String JSON_PAR = "\"[^\"\\\\]*\"\\s*:\\s*" + JSON_VALOR;
    static final Pattern JSON_PLANO = Pattern.compile("\\{\\s*(?:" + JSON_PAR + "(?:\\s*,\\s*" + JSON_PAR + ")*)?\\s*\\}");
    static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        int porta = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        recursos.put("R1", new Recurso("R1"));                    // o servidor ja cria R1 e R2
        recursos.put("R2", new Recurso("R2"));
        HttpServer srv = HttpServer.create(new InetSocketAddress(porta), 0); // aceita conexoes de outras maquinas
        srv.createContext("/", ServidorCentral::tratar);
        srv.setExecutor(Executors.newCachedThreadPool());          // uma thread por requisicao (SSE nao trava as demais)
        srv.start();
        log("Servidor central em http://0.0.0.0:" + porta + "  (recursos: R1, R2)");
    }

    // ------------------------------------------------------------------ roteamento

    /** Recebe toda requisicao, escolhe a operacao pela rota/verbo, responde e registra no log. */
    static void tratar(HttpExchange ex) throws IOException {
        String metodo = ex.getRequestMethod();
        String[] p = ex.getRequestURI().getPath().split("/");      // ["", "recursos", id, "acesso"]
        String cliente = consulta(ex, "cliente");
        String recurso = p.length > 2 ? p[2] : "-";
        Resp r;
        try {
            String corpo = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (p.length == 2 && p[1].equals("eventos") && metodo.equals("GET")) {
                eventos(ex, cliente);                              // SSE: conexao fica aberta, nao responde aqui
                return;
            } else if (p.length == 2 && p[1].equals("recursos") && metodo.equals("GET")) {
                r = listar();
            } else if (p.length == 3 && p[1].equals("recursos") && metodo.equals("GET")) {
                r = consultar(p[2]);
            } else if (p.length == 4 && p[1].equals("recursos") && p[3].equals("acesso") && metodo.equals("POST")) {
                if (!JSON_PLANO.matcher(corpo.trim()).matches()) {   // corpo que nao e JSON valido -> 400
                    r = erro(400, "corpo malformado: esperado JSON {\"cliente\":\"NomeDoCliente\"}");
                } else {
                    cliente = campo(corpo, "cliente");
                    r = pedir(p[2], cliente);
                }
            } else if (p.length == 4 && p[1].equals("recursos") && p[3].equals("acesso") && metodo.equals("DELETE")) {
                r = liberar(p[2], cliente);
            } else if (p.length >= 2 && p[1].equals("recursos")) {
                r = erro(405, "rota ou metodo nao suportado");
            } else {
                r = erro(404, "rota inexistente");
            }
        } catch (Exception e) {
            r = erro(500, e.toString());
        }
        enviar(ex, r);
        log(String.format("REQ  [%s] %s %s cliente=%s recurso=%s -> %d %s", Thread.currentThread().getName(), metodo,
                ex.getRequestURI().getPath(), cliente, recurso, r.status, r.corpo));
    }

    // ------------------------------------------------------------------ operacoes

    /** Pedir acesso: concede se livre; senao entra no fim da fila e responde na hora (202). */
    static Resp pedir(String rid, String cliente) {
        if (cliente == null || !NOME.matcher(cliente).matches())
            return erro(400, "campo 'cliente' obrigatorio (letras, numeros, _ ou -)");
        synchronized (LOCK) {
            Recurso r = recursos.get(rid);
            if (r == null) return erro(404, "recurso inexistente");

            if (cliente.equals(r.dono))                            // ja e o dono (ex.: foi notificado e pediu de novo):
                return new Resp(200, estadoPedido("concedido", r, cliente, 0)); // mantem a vez, nao duplica nada
            int pos = r.fila.indexOf(cliente);
            if (pos >= 0)                                          // ja esta na fila: nao duplica, devolve a posicao atual
                return new Resp(202, estadoPedido("espera", r, cliente, pos + 1));

            for (Recurso o : recursos.values())                    // POLITICA ANTI-IMPASSE: 1 recurso por cliente
                if (o != r && (cliente.equals(o.dono) || o.fila.contains(cliente)))
                    return new Resp(409, "{\"erro\":\"politica anti-impasse: " + cliente
                            + " ja detem ou espera " + o.id + "; libere antes\"}");

            if (r.dono == null) {                                  // livre: concede
                r.dono = cliente;
                log("CONCESSAO " + rid + " -> " + cliente);
                return new Resp(200, estadoPedido("concedido", r, cliente, 0));
            }
            if (!fluxos.containsKey(cliente))                      // sem canal SSE nao ha como avisar depois
                return erro(412, "abra GET /eventos?cliente=" + cliente + " antes de entrar na fila");
            r.fila.addLast(cliente);                               // ocupado: registra interesse (fila FIFO)
            log("FILA      " + rid + " <- " + cliente + " (posicao " + r.fila.size() + ", dono " + r.dono + ")");
            return new Resp(202, estadoPedido("espera", r, cliente, r.fila.size()));
        }
    }

    /** Liberar: so o dono. Repassa o token ao primeiro da fila (ja o torna dono) e notifica depois do lock. */
    static Resp liberar(String rid, String cliente) {
        if (cliente == null || !NOME.matcher(cliente).matches())
            return erro(400, "parametro 'cliente' obrigatorio");
        String proximo;
        OutputStream fluxo = null;
        synchronized (LOCK) {
            Recurso r = recursos.get(rid);
            if (r == null) return erro(404, "recurso inexistente");
            if (!cliente.equals(r.dono))
                return erro(403, "apenas o dono pode liberar (dono atual: " + r.dono + ")");
            proximo = r.fila.pollFirst();                          // retira o primeiro da fila (null se vazia)
            r.dono = proximo;                                      // repasse do token: ja e o novo dono ANTES de ser avisado
            if (proximo != null) fluxo = fluxos.get(proximo);
            log("LIBERACAO " + rid + " por " + cliente
                    + (proximo == null ? " -> recurso livre" : " -> REPASSE do token para " + proximo));
        }
        // ---- fora do lock ----
        if (proximo != null) notificar(rid, proximo, fluxo);
        return new Resp(200, "{\"liberado\":true,\"recurso\":\"" + rid + "\",\"novoDono\":"
                + (proximo == null ? "null" : "\"" + proximo + "\"") + "}");
    }

    static Resp listar() {
        synchronized (LOCK) {
            StringBuilder sb = new StringBuilder("[");
            for (Recurso r : recursos.values()) { if (sb.length() > 1) sb.append(","); sb.append(json(r)); }
            return new Resp(200, sb.append("]").toString());
        }
    }

    static Resp consultar(String rid) {
        synchronized (LOCK) {
            Recurso r = recursos.get(rid);
            return r == null ? erro(404, "recurso inexistente") : new Resp(200, json(r));
        }
    }

    
/** SSE: o cliente mantem este GET aberto; o servidor escreve os eventos "concedido". */
static void eventos(HttpExchange ex, String cliente) throws IOException {

    if (cliente == null || !NOME.matcher(cliente).matches()) {
        enviar(ex, erro(400, "parametro 'cliente' obrigatorio"));
        return;
    }

    ex.getResponseHeaders().set("Content-Type", "text/event-stream");
    ex.getResponseHeaders().set("Cache-Control", "no-cache");
    
    synchronized (LOCK) {
        // Estabelece a conexao SSE
        ex.sendResponseHeaders(200, 0);

        OutputStream os = ex.getResponseBody();

        // Envia confirmacao inicial da conexao
        synchronized (os) {
            os.write(": conectado\n\n".getBytes(StandardCharsets.UTF_8));
            os.flush();
        }

        // Registra o canal antes de liberar o LOCK
        fluxos.put(cliente, os);

        log("SSE       " + cliente + " conectado ao canal de notificacoes ["
                + Thread.currentThread().getName() + "]");
    }
}


    // ------------------------------------------------------------------ notificacao (publisher)

    /** Publica o evento "concedido" ao assinante. Roda em outra thread: o servidor nao espera o cliente. */
    static void notificar(String rid, String cliente, OutputStream fluxo) {
        notificador.submit(() -> {
            String dados = "{\"recurso\":\"" + rid + "\",\"evento\":\"concedido\",\"cliente\":\"" + cliente + "\"}";
            if (fluxo == null) { log("NOTIF FALHOU " + cliente + ": sem canal SSE aberto"); return; }
            try {
                synchronized (fluxo) {
                    fluxo.write(("event: concedido\ndata: " + dados + "\n\n").getBytes(StandardCharsets.UTF_8));
                    fluxo.flush();                                 // flush apos cada evento, senao nao chega
                }
                log("NOTIF     enviada a " + cliente + ": " + dados + " [" + Thread.currentThread().getName() + "]");
            } catch (IOException e) {
                synchronized (LOCK) { fluxos.remove(cliente, fluxo); }
                log("NOTIF FALHOU " + cliente + ": " + e);
            }
        });
    }

    // ------------------------------------------------------------------ utilitarios

    static String json(Recurso r) {                                // chamar com LOCK preso
        StringBuilder f = new StringBuilder("[");
        for (String c : r.fila) { if (f.length() > 1) f.append(","); f.append("\"").append(c).append("\""); }
        return "{\"id\":\"" + r.id + "\",\"dono\":" + (r.dono == null ? "null" : "\"" + r.dono + "\"")
                + ",\"fila\":" + f.append("]") + "}";
    }

    static String estadoPedido(String status, Recurso r, String cliente, int posicao) {
        return "{\"status\":\"" + status + "\",\"recurso\":\"" + r.id + "\",\"cliente\":\"" + cliente
                + "\",\"posicao\":" + posicao + "}";
    }

    static String campo(String json, String nome) {                // le {"nome":"valor"} (JSON plano)
        Matcher m = Pattern.compile("\"" + nome + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    static String consulta(HttpExchange ex, String nome) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return null;
        for (String par : q.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv[0].equals(nome) && kv.length == 2) return kv[1];
        }
        return null;
    }

    static Resp erro(int status, String msg) {
        return new Resp(status, "{\"erro\":\"" + msg.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}");
    }

    static void enviar(HttpExchange ex, Resp r) throws IOException {
        byte[] b = r.corpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(r.status, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    static synchronized void log(String msg) {
        System.out.println("[" + LocalTime.now().format(HORA) + "] " + msg);
    }
}