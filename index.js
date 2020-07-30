const fs = require('fs-extra');
const csvParse = require("csv-parse/lib/sync");
const xmlParse = require("xml2js");
const { default: Axios } = require('axios');
const geohash = require('ngeohash');
const path = require('path');

const userDesktop = path.join(require('os').homedir(), 'Desktop');

const NaPTANUrlCSV = 'http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv&LA=370'; // NaPTAN data for South Yorkshire (Atco Prefix 370)
const NaPTANUrlXML = 'http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=xml&LA=370'; // NaPTAN data for South Yorkshire (Atco Prefix 370)

const administrativeAreaCodes = {"110": {name: "National - National Rail"}, "143": {name: "National - National Coach"}, "145": {name: "National - National Air"}, "146": {name: "National - National Ferry"}, "147": {name: "National - National Tram"}};

let run = async () => {
    // const NaPTANCSV = await Axios.get(NaPTANUrlCSV);
    // const NaPTANXML = await Axios.get(NaPTANUrlXML);

    const NaPTANCSV = await fs.readFile(path.join(userDesktop, 'Stops.csv'));

    const CSVrecords = csvParse(NaPTANCSV, {
        columns: true,
        skip_empty_lines: true
    });

    let outputRecords = {};

    for(let record in CSVrecords) {
        record = CSVrecords[record];

        if(record['Status'] === "act" &&
        (((record['StopType'] === "TMU" || record['StopType'] === "MET" || record['StopType'] === "PLT") && record['AdministrativeAreaCode'] === "147") ||
        (record['StopType'] !== "TMU" || record['StopType'] !== "MET" || record['StopType'] !== "PLT") && record['AdministrativeAreaCode'] === "099")) {
            console.log(record['CommonName']);

            if(administrativeAreaCodes[record['AdministrativeAreaCode']]) {
                record['AdministrativeAreaCode'] = administrativeAreaCodes[record['AdministrativeAreaCode']]['name'];
            }

            let object = {};
            object['name'] = record['CommonName'];
            object['atco'] = record['ATCOCode'];
            object['naptan'] = record['NaptanCode'];
            object['plate'] = record['PlateCode'];
            object['type'] = record['StopType'];
            object['geohash'] = geohash.encode(record['Latitude'], record['Longitude']);
            object['location'] = {lat: record['Latitude'], lon: record['Longitude']};
            object['record'] = record;

            let topSortLevel = record['GrandParentLocalityName'] !== "" ? record['GrandParentLocalityName'] : record['ParentLocalityName'] !== "" ? record['ParentLocalityName'] : record['Town'] !== "" ? record['Town'] : record['LocalityName'];

            outputRecords[record['AdministrativeAreaCode']] = outputRecords[record['AdministrativeAreaCode']] || {};
            outputRecords[record['AdministrativeAreaCode']][topSortLevel] = outputRecords[record['AdministrativeAreaCode']][topSortLevel] || {};

            outputRecords[record['AdministrativeAreaCode']][topSortLevel][record['ATCOCode']] = object;
        }
    }

    // let output = fs.createWriteStream(path.join(userDesktop, "NaPTANXML.response.zip"));

    // const XMLrecords = await xmlParse.parseStringPromise(NaPTANXML.data.replace("\ufeff", ""));

    await fs.writeFile(path.join(userDesktop, "NaPTANCSV.json"), JSON.stringify(outputRecords, null, 1));

    // await fs.writeFile(path.join(userDesktop, "NaPTANXML.json"), JSON.stringify(XMLrecords));
}

run().then(() => {
    console.log("Yay");
}).catch((e) => {
    console.log("Damnn")
    console.log(e);
});