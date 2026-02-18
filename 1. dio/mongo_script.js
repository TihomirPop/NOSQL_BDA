// 0. import u MongoDB
// mongoimport --uri="mongodb://localhost:27017" --db nosql --collection flags --type csv --file dataset/flag.data --fieldFile dataset/flag.fieldfile

// 1.   Sve nedostajuće vrijednosti kontinuirane varijable zamijeniti sa -1, a kategoričke sa „empty“.

const kontinuiraneVarijable = [
    "area", "population", "bars", "stripes", "colours",
    "circles", "crosses", "saltires", "quarters", "sunstars"
];

const kategorickeVarijable = [
    "name", "landmass", "zone", "language", "religion",
    "red", "green", "blue", "gold", "white",
    "black", "orange", "mainhue", "crescent", "triangle",
    "icon", "animate", "text", "topleft", "botright"
];

// U ovom setu podataka nema nedostajućih vrijednosti, ali kada bi ih bilo, mogli bismo ih zamijeniti ovako:
kontinuiraneVarijable.forEach(v => {
    const res = db.flags.updateMany(
        { [v]: { $exists: false } },
        { $set: { [v]: -1 } }
    );
    print(`${v}: ${res.modifiedCount} zapisa ažurirano na -1`);
});

kategorickeVarijable.forEach(v => {
    const res = db.flags.updateMany(
        { [v]: { $exists: false } },
        { $set: { [v]: "empty" } }
    );
    print(`${v}: ${res.modifiedCount} zapisa ažurirano na "empty"`);
});