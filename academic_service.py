import json, os, time, shutil
from datetime import datetime, timezone


BASE = os.path.dirname(__file__)
INBOX = os.path.join(BASE, "inbox")
OUTBOX = os.path.join(BASE, "outbox")
PROCESSED = os.path.join(BASE, "processed")
DEAD = os.path.join(BASE, "deadletter")


def now_iso():
   return datetime.now(timezone.utc).isoformat()


def ensure_dirs():
   for p in [INBOX, OUTBOX, PROCESSED, DEAD]:
       os.makedirs(p, exist_ok=True)


def load_json(path):
   with open(path, "r", encoding="utf-8") as f:
       return json.load(f)


def write_outbox(evt: dict):
   fname = f"{evt['ts'].replace(':','-')}_{evt['event_id']}.json"
   path = os.path.join(OUTBOX, fname)
   with open(path, "w", encoding="utf-8") as f:
       json.dump(evt, f, ensure_ascii=False)


def move(path, folder):
   os.makedirs(folder, exist_ok=True)
   shutil.move(path, os.path.join(folder, os.path.basename(path)))


def main():
   ensure_dirs()


   # Modo: "tempo real" -> POLL_SECONDS=1 | "lote" -> POLL_SECONDS=120
   POLL_SECONDS = int(os.getenv("POLL_SECONDS", "1"))


   # Vagas por disciplina (simples)
   seats = {"BD101": 5, "ENG200": 3, "MAT150": 2}


   # Idempotência: event_id já processados
   processed_ids_path = os.path.join(BASE, "processed_ids.txt")
   processed_ids = set()
   if os.path.exists(processed_ids_path):
       with open(processed_ids_path, "r", encoding="utf-8") as f:
           processed_ids = set(line.strip() for line in f if line.strip())


   print(f"academic_service iniciado. POLL_SECONDS={POLL_SECONDS}. Vagas iniciais={seats}")


   while True:
       files = sorted([f for f in os.listdir(INBOX) if f.endswith(".json")])


       if not files:
           time.sleep(POLL_SECONDS)
           continue


       for fname in files:
           path = os.path.join(INBOX, fname)
           try:
               evt = load_json(path)


               # Validação mínima
               if evt.get("type") != "SolicitacaoMatriculaCriada":
                   raise ValueError("Evento desconhecido")
               for k in ["event_id", "request_id", "student_id", "course_id", "term", "credits"]:
                   if k not in evt:
                       raise ValueError(f"Campo obrigatório ausente: {k}")
               if not isinstance(evt["credits"], int) or evt["credits"] <= 0:
                   raise ValueError("credits inválido (deve ser inteiro > 0)")


               # Idempotência (ignorar duplicados)
               if evt["event_id"] in processed_ids:
                   print(f"[B] IGNORADO (duplicado) event_id={evt['event_id']} request_id={evt['request_id']}")
                   move(path, PROCESSED)
                   continue


               course = evt["course_id"]
               available = seats.get(course, 0)


               if available > 0:
                   seats[course] = available - 1
                   status = "Matriculado"
                   approved = True
               else:
                   status = "SemVagas"
                   approved = False


               out_evt = {
                   "event_id": evt["event_id"],  # rastreabilidade
                   "type": "ResultadoMatricula",
                   "ts": now_iso(),
                   "request_id": evt["request_id"],
                   "student_id": evt["student_id"],
                   "course_id": evt["course_id"],
                   "term": evt["term"],
                   "credits": evt["credits"],
                   "approved": approved,
                   "status": status,
                   "seats_after": seats.get(course, 0),
               }
               write_outbox(out_evt)


               processed_ids.add(evt["event_id"])
               with open(processed_ids_path, "a", encoding="utf-8") as f:
                   f.write(evt["event_id"] + "\n")


               print(f"[B] {evt['request_id']} -> {status} approved={approved} seats_after={out_evt['seats_after']}")
               move(path, PROCESSED)


           except Exception as e:
               print(f"[B] ERRO em {fname}: {e}")
               move(path, DEAD)


       time.sleep(POLL_SECONDS)


if __name__ == "__main__":
   main()
