import json, os, time, uuid, random
from datetime import datetime, timezone


BASE = os.path.dirname(__file__)
INBOX = os.path.join(BASE, "inbox")


def now_iso():
   return datetime.now(timezone.utc).isoformat()


def write_event(evt: dict):
   os.makedirs(INBOX, exist_ok=True)
   fname = f"{evt['ts'].replace(':','-')}_{evt['event_id']}.json"
   path = os.path.join(INBOX, fname)
   with open(path, "w", encoding="utf-8") as f:
       json.dump(evt, f, ensure_ascii=False)


def main():
   print("enrollment_service iniciado. Gerando solicitações em inbox/ ...")


   students = ["A100", "A101", "A102", "A103", "A104"]
   courses = ["BD101", "ENG200", "MAT150"]
   terms = ["2026.1"]


   # Ajustes didáticos
   total_events = 20
   interval_seconds = 4
   duplicate_rate = 0.20
   invalid_rate = 0.10


   last_evt = None


   for i in range(total_events):
       evt = {
           "event_id": str(uuid.uuid4()),
           "type": "SolicitacaoMatriculaCriada",
           "ts": now_iso(),
           "request_id": f"R{2000+i}",
           "student_id": random.choice(students),
           "course_id": random.choice(courses),
           "term": random.choice(terms),
           "credits": random.choice([2, 4, 6]),
       }


       # Evento inválido (credits <= 0)
       if random.random() < invalid_rate:
           evt["credits"] = 0


       write_event(evt)
       print(f"[A] Solicitação -> {evt['request_id']} aluno={evt['student_id']} curso={evt['course_id']} credits={evt['credits']}")
       last_evt = evt


       # Duplicidade (reenvio do mesmo event_id)
       if last_evt and random.random() < duplicate_rate:
           dup = dict(last_evt)
           dup["ts"] = now_iso()
           write_event(dup)
           print(f"[A] DUPLICADO -> request_id={dup['request_id']} event_id={dup['event_id']}")


       time.sleep(interval_seconds)


   print("enrollment_service finalizado.")


if __name__ == "__main__":
   main()
