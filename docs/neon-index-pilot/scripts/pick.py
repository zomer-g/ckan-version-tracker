import sys, json
sys.stdout.reconfigure(encoding="utf-8")
d = json.load(open("survey.json", encoding="utf-8"))
by_id = {c["dataset_id"]: c for c in d}


def near(pool, target, used):
    return min((c for c in pool if c["dataset_id"] not in used),
               key=lambda c: abs(c["size"] - target))


gm = [c for c in d if c["source_type"] == "govmap"]
sc = [c for c in d if c["source_type"] == "scraper"]
picked, used = [], set()


def take(c, note):
    if c["dataset_id"] in used:
        return
    used.add(c["dataset_id"])
    c = dict(c)
    c["note"] = note
    picked.append(c)


# GovMap — ceiling, then a spread down to the long tail
take(max(gm, key=lambda c: c["size"]), "govmap ceiling (largest layer in corpus)")
take(near(gm, 258 * 2**20, used), "govmap very large")
take(near(gm, 70 * 2**20, used), "govmap large")
take(near(gm, 29 * 2**20, used), "govmap medium")
take(by_id["304e43d5-c419-43bd-8b46-f31a4da0c075"], "govmap small (user's example)")
take(near(gm, 300 * 2**10, used), "govmap tiny")
take(near(gm, 5 * 2**10, used), "govmap micro (corpus median)")

# Scrapers — the two big document corpora + the long-tail shape
take(by_id["82db2f91-9ff0-44f4-b842-df3c43f7185a"], "scraper FOI answers (user's example)")
take(near([c for c in sc if c["kind"] is None], 133 * 2**20, used), "scraper very large (gov decisions)")
take(near([c for c in sc if c["kind"] == "knesset"], 20 * 2**10, used), "scraper knesset protocol (long tail, 1921 like it)")

json.dump(picked, open("pilot_set.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print(f"{'MB':>10}  {'type':<7} {'title':<42} note")
for c in picked:
    print(f"{c['size']/2**20:10.4f}  {c['source_type']:<7} {c['title'][:42]:<42} {c['note']}")
print(f"\ntotal pilot bytes: {sum(c['size'] for c in picked)/2**30:.3f} GB")
