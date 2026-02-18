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
    print(`${v}: ${res.modifiedCount} dokumenata ažurirano na -1`);
});

kategorickeVarijable.forEach(v => {
    const res = db.flags.updateMany(
        { [v]: { $exists: false } },
        { $set: { [v]: "empty" } }
    );
    print(`${v}: ${res.modifiedCount} dokumenata ažurirano na "empty"`);
});

// 2.   Za svaku kontinuiranu vrijednost izračunati srednju vrijednost, standardnu devijaciju i kreirati novi dokument oblika
//  sa vrijednostima, dokument nazvati:  statistika_ {ime vašeg data seta}. U izračun se uzimaju samo nomissingvrijednosti.

db.statistika_flags.drop(); // Brisanje kolekcija kako bismo mogli ponovo pokrenuti skriptu bez duplikata

kontinuiraneVarijable.forEach(v => {
    db.flags.aggregate([
        {
            $match: { [v]: { $ne: -1 } }
        },
        {
            $group: {
                _id: null,
                average: { $avg: `$${v}` },
                standardDeviation: { $stdDevPop: `$${v}` },
                count: { $sum: 1 }
            }
        },
        {
            $project: {
                _id: 0,
                variable: v,
                average: 1,
                standardDeviation: 1,
                count: 1
            }
        },
        {
            $merge: { into: "statistika_flags" }
        }
    ]);
});

// 3.	Za svaku kategoričku  vrijednost izračunati frekvencije pojavnosti po obilježjima varijabli i kreirati novi dokument koristeći nizove,
// dokument nazvati:  frekvencija_ {ime vašeg data seta} . Frekvencije računati koristeći $inc modifikator. 

db.frekvencija_flags.drop();

kategorickeVarijable.forEach(v => {
    db.flags.find().forEach(doc => {
        const val = doc[v]
        db.frekvencija_flags.updateOne(
            { variable: v },
            { $inc: { [`counts.${val}`]: 1 } },
            { upsert: true } // Kreiraj ako ne postoji
        );
    });
});