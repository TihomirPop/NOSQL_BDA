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

print("Statistika za kontinuirane varijable izračunata i spremljena u kolekciju 'statistika_flags'.");



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

print("Frekvencije za kategoričke varijable izračunate i spremljene u kolekciju 'frekvencija_flags'.");



// 4.	Iz osnovnog  dokumenta kreirati dva nova dokumenta sa kontinuiranim vrijednostima u kojoj će u 
// prvom dokumentu   biti sadržani svi elementi <= srednje vrijednosti , a u drugom dokumentu biti 
// sadržani svi elementi >srednje vrijednosti , dokument nazvati:  statistika1_ {ime vašeg data seta} 
// i  statistika2_ {ime vašeg data seta} 

db.statistika1_flags.drop();

kontinuiraneVarijable.forEach(v => {
    db.flags.aggregate([
        {
            $match: { [v]: { $lte: db.statistika_flags.findOne({ variable: v }).average } }
        },
        {
            $group: {
                _id: null,
                values: { $push: `$${v}` }
            }
        },
        {
            $project: {
                _id: 0,
                variable: v,
                values: 1
            }
        },
        {
            $merge: { into: "statistika1_flags" }
        }
    ]);
})

print("Dokumenti sa vrijednostima <= srednjoj vrijednosti kreirani i spremljeni u kolekciju 'statistika1_flags'.");

db.statistika2_flags.drop();

kontinuiraneVarijable.forEach(v => {
    db.flags.aggregate([
        {
            $match: { [v]: { $gt: db.statistika_flags.findOne({ variable: v }).average } }
        },
        {
            $group: {
                _id: null,
                values: { $push: `$${v}` }
            }
        },
        {
            $project: {
                _id: 0,
                variable: v,
                values: 1
            }
        },
        {
            $merge: { into: "statistika2_flags" }
        }
    ]);
})

print("Dokumenti sa vrijednostima > srednjoj vrijednosti kreirani i spremljeni u kolekciju 'statistika2_flags'.");



//5.    Osnovni  dokument  kopirati u novi te embedati vrijednosti iz tablice 3 za svaku 
// kategoričku vrijednost, :  emb_ {ime vašeg data seta} 

db.emb_flags.drop();

db.flags.find().forEach(doc => {
    const newDoc = { ...doc };
    kategorickeVarijable.forEach(v => {
        const val = doc[v];
        const freqDoc = db.frekvencija_flags.findOne({ variable: v });
        newDoc[`freq_${v}`] = freqDoc.counts[val];
    });
    db.emb_flags.insertOne(newDoc);
});

print("Novi dokumenti s embedanim frekvencijama kreirani i spremljeni u kolekciju 'emb_flags'.");


// 6.	Osnovni  dokument  kopirati u novi te embedati vrijednosti iz tablice 2 za svaku 
// kontinuiranu  vrijednost kao niz :  emb2_ {ime vašeg data seta} 

db.emb2_flags.drop();

db.flags.find().forEach(doc => {
    const newDoc = { ...doc };
    kontinuiraneVarijable.forEach(v => {
        const statDoc = db.statistika_flags.findOne({ variable: v });
        newDoc[`stats_${v}`] = [statDoc.average, statDoc.standardDeviation, statDoc.count];
    });
    db.emb2_flags.insertOne(newDoc);
});

print("Novi dokumenti s embedanim statistikama kreirani i spremljeni u kolekciju 'emb2_flags'.");