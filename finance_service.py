import json, os, time, shutil
from datetime import datetime, timezone


BASE = os.path.dirname(__file__)
OUTBOX = os.path.join(BASE, "outbox")
PROCESSED = os.path.join(BASE, "processed")


def now_iso():
   return datetime.now(timezone.utc).isoformat()


def ensure_dirs():
   for p in [OUTBOX, PROCESSED]:
       os.makedirs(p, exist_ok=True)


def load_json(path):
   with open(path, "r", encoding="utf-8") as f:
       return json.load(f)


def move(path, folder):
   os.makedirs(folder, exist_ok=True)
   shutil.move(path, os.path.join(folder, os.path.basename(path)))


def main():
   ensure_dirs()
   poll = int(os.getenv("FIN_POLL_SECONDS", "1"))
   log_path = os.path.join(BASE, "finance_log.csv")


   if not os.path.exists(log_path):
       with open(log_path, "w", encoding="utf-8") as f:
           f.write("ts,request_id,student_id,course_id,term,approved,status,credits\n")


   print(f"finance_service iniciado. FIN_POLL_SECONDS={poll}. Log: finance_log.csv")


   while True:
       files = sorted([f for f in os.listdir(OUTBOX) if f.endswith(".json")])


       for fname in files:
           path = os.path.join(OUTBOX, fname)
           evt = load_json(path)


           if evt.get("type") == "ResultadoMatricula":
               with open(log_path, "a", encoding="utf-8") as f:
                   f.write(f"{now_iso()},{evt['request_id']},{evt['student_id']},{evt['course_id']},{evt['term']},{evt['approved']},{evt['status']},{evt['credits']}\n")


               print(f"[C] Registrado: {evt['request_id']} approved={evt['approved']} status={evt['status']}")
               move(path, PROCESSED)
           else:
               print(f"[C] Ignorado (tipo desconhecido): {fname}")
               move(path, PROCESSED)


       time.sleep(poll)


if __name__ == "__main__":
   main()
