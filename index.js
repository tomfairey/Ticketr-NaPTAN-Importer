const fs = require('fs-extra');
const csvParse = require("csv-parse/lib/sync");
const xml2js = require("xml2js");
const { default: Axios } = require('axios');
const FileDownload = require('js-file-download');
const geohash = require('ngeohash');
const path = require('path');
const os = require('os');
const extract = require('extract-zip');
const ngeohash = require('ngeohash');
const mariadb = require('mariadb');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
require('dotenv').config();

// const folderName = `Ticketr-NaPTAN-Importer-${(new Date()).toISOString()}`;
// const storagePath = path.join(os.homedir(), 'Desktop', folderName);
// const fileName = `NaPTAN-${(new Date()).toISOString()}`;
// const fileNameWithExtension = `${fileName}.zip`;

// const NaPTANUrlCSV = 'http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv&LA=370|910|940'; // NaPTAN data for South Yorkshire (Atco Prefix 370) / National Rail / National Trams
// const NaPTANUrlXML = 'http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=xml&LA=370|910|940'; // NaPTAN data for South Yorkshire (Atco Prefix 370) / National Rail / National Trams

const administrativeAreaCodes = {"110": {name: "National - National Rail"}, "143": {name: "National - National Coach"}, "145": {name: "National - National Air"}, "146": {name: "National - National Ferry"}, "147": {name: "National - National Tram"}};

let run = async () => {
    // const NaPTANXMLArchive = await Axios.get(`${NaPTANUrlXML}`, {method: 'GET', responseType: 'stream'});

    // console.log(NaPTANXMLArchive.data);

    // await fs.mkdir(storagePath, {recursive: true});

    // await NaPTANXMLArchive.data.pipe(fs.createWriteStream(path.join(storagePath, fileNameWithExtension)))

    // await FileDownload(NaPTANXMLArchive.data, path.join(storagePath, fileNameWithExtension));

    // await extract(fileNameWithExtension, { dir: path.join(storagePath, fileName) });

    let NPTGXMLPath = 'Nptgxml';
    // let NaPTANXMLPath = 'D:\\Downloads\\DATA_142421';
    let NaPTANXMLPath = 'NaPTANxml';

    let currentTimestamp = Math.round(Date.now() / 1000);

    let outputObject = {};

    let filesNPTG = await fs.readdir(NPTGXMLPath);

    for(let file in filesNPTG) {
        file = filesNPTG[file];

        console.log("Found:", file, "for areas...");

        let parser = new xml2js.Parser();

        let data = await fs.readFile(path.join(NPTGXMLPath, file));

        let result = await parser.parseStringPromise(data);

        let NationalPublicTransportGazetteer = result['NationalPublicTransportGazetteer'];
        for(let region in NationalPublicTransportGazetteer['Regions'][0]['Region']) {
            region = NationalPublicTransportGazetteer['Regions'][0]['Region'][region];

            let region_code = region['RegionCode'][0];

            outputObject['regions'] ? null : outputObject["regions"] = {};
            outputObject['regions'][region_code] = {};

            outputObject['regions'][region_code]['region_code'] = region_code;
            outputObject['regions'][region_code]['name'] = region['Name'][0]['_'] || region['Name'][0];
            outputObject['regions'][region_code]['country'] = region['Country'][0];

            for(let administrative_area in region['AdministrativeAreas'][0]['AdministrativeArea']) {
                administrative_area = region['AdministrativeAreas'][0]['AdministrativeArea'][administrative_area];

                let administrative_area_code = administrative_area['AdministrativeAreaCode'][0];

                outputObject['administrative_areas'] ? null : outputObject["administrative_areas"] = {};
                outputObject['administrative_areas'][administrative_area_code] = {};

                outputObject['administrative_areas'][administrative_area_code]['administrative_area_code'] = administrative_area_code;
                outputObject['administrative_areas'][administrative_area_code]['atco_area_code'] = administrative_area['AtcoAreaCode'][0];
                outputObject['administrative_areas'][administrative_area_code]['name'] = administrative_area['Name'][0];
                outputObject['administrative_areas'][administrative_area_code]['short_name'] = administrative_area['ShortName'][0];
                outputObject['administrative_areas'][administrative_area_code]['region_code'] = region_code;

                if(administrative_area['NptgDistricts'] && administrative_area['NptgDistricts'].length) {
                    for(let nptg_district in administrative_area['NptgDistricts'][0]['NptgDistrict']) {
                        nptg_district = administrative_area['NptgDistricts'][0]['NptgDistrict'][nptg_district];

                        let nptg_district_code = nptg_district['NptgDistrictCode'][0];

                        outputObject['nptg_districts'] ? null : outputObject["nptg_districts"] = {};
                        outputObject['nptg_districts'][nptg_district_code] = {};

                        outputObject['nptg_districts'][nptg_district_code]['nptg_district_code'] = nptg_district_code;
                        outputObject['nptg_districts'][nptg_district_code]['name'] = nptg_district['Name'][0];
                        outputObject['nptg_districts'][nptg_district_code]['administrative_area_reference'] = administrative_area_code;
                    }
                }
            }

            for(let nptg_locality in NationalPublicTransportGazetteer['NptgLocalities'][0]['NptgLocality']) {
                nptg_locality = NationalPublicTransportGazetteer['NptgLocalities'][0]['NptgLocality'][nptg_locality];

                let nptg_locality_code = nptg_locality['NptgLocalityCode'][0];

                outputObject['nptg_localities'] ? null : outputObject["nptg_localities"] = {};
                outputObject['nptg_localities'][nptg_locality_code] = {};

                outputObject['nptg_localities'][nptg_locality_code]['nptg_locality_code'] = nptg_locality_code;
                outputObject['nptg_localities'][nptg_locality_code]['name'] = nptg_locality['Descriptor'][0]['LocalityName'][0]['_'];
                outputObject['nptg_localities'][nptg_locality_code]['short_name'] = nptg_locality['Descriptor'][0]['ShortName'] ? nptg_locality['Descriptor'][0]['ShortName'][0] : null;
                outputObject['nptg_localities'][nptg_locality_code]['qualifier_name'] = nptg_locality['Descriptor'][0]['Qualify'] ? nptg_locality['Descriptor'][0]['Qualify'][0]['QualifierName'][0]['_'] : null;

                outputObject['nptg_localities'][nptg_locality_code]['parent_nptg_locality_reference'] = nptg_locality['ParentNptgLocalityRef'] ? nptg_locality['ParentNptgLocalityRef'][0]['_'] : null;
                outputObject['nptg_localities'][nptg_locality_code]['administrative_area_reference'] = nptg_locality['AdministrativeAreaRef'][0];
                outputObject['nptg_localities'][nptg_locality_code]['nptg_district_reference'] = nptg_locality['NptgDistrictRef'][0];
                outputObject['nptg_localities'][nptg_locality_code]['source_locality_type'] = nptg_locality['SourceLocalityType'][0];

                outputObject['nptg_localities'][nptg_locality_code]['latitude'] = nptg_locality['Location'] ? nptg_locality['Location'][0]['Translation'][0]['Latitude'][0] : null;
                outputObject['nptg_localities'][nptg_locality_code]['longitude'] = nptg_locality['Location'] ? nptg_locality['Location'][0]['Translation'][0]['Longitude'][0] : null;
            }
        }

        console.log("Completed:", file, "B¬)");
    };

    let count = 0;
    let maxCount = 5;
    let maxMessage = false;

    let getAllBoolean = process.argv[2] === "--get-all";
    let getSingleValue = process.argv[2] !== "--get-all" ? process.argv[2] : null;

    if(getAllBoolean) {
        for(let area in outputObject['administrative_areas']) {
            area = outputObject['administrative_areas'][area];
            if(area.atco_area_code !== 900) {
                if(count <= maxCount) {
                    await exec(`wget "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=xml&LA=${area.atco_area_code}" -O "${area.atco_area_code}.zip"`);
                    await exec(`unzip "${area.atco_area_code}.zip" -d "${NaPTANXMLPath}"`);
                } else {
                    if(!maxMessage) {
                        console.log(`Max of`, maxCount, `areas reached!`);
                        maxMessage = true;
                    }
                }
                count ++;
            }
        }
    } else if(getSingleValue) {
        await exec(`wget "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=xml&LA=${getSingleValue}" -O "${getSingleValue}.zip"`);
        await exec(`unzip "${getSingleValue}.zip" -d "${getSingleValue}"`);
    }

    let filesNaPTAN = await fs.readdir(NaPTANXMLPath);

    for(let file in filesNaPTAN) {
        file = filesNaPTAN[file];
        console.log("Found:", file, "for stops...");

        let parser = new xml2js.Parser();

        let data = await fs.readFile(path.join(NaPTANXMLPath, file));

        let result = await parser.parseStringPromise(data);

        let NaPTAN = result['NaPTAN'];

        for(let stopPoint in NaPTAN['StopPoints'][0]['StopPoint']) {
            stopPoint = NaPTAN['StopPoints'][0]['StopPoint'][stopPoint];

            if(stopPoint['$']['Status'] === "active") {

                let stopType = null;
                let stop_type_code = stopPoint['StopClassification'] ? stopPoint['StopClassification'][0]['StopType'][0] : null
                let default_method_accuracy = null;
                let continueWithThisStopInThisAdministrativeAreaRef = false;

                switch(stopPoint['AdministrativeAreaRef'][0]) {
                    case "110":
                        stopType = "train_station";
                        default_method_accuracy = 100;

                            // Don't add anything other than each actual station 'access area'
                            // as the data contains the relevant identifiers for timetables,
                            // etc. while plaforms do not have this and would just be "ghost"
                            // stops.

                        switch(stop_type_code) {
                            case "RLY":
                                continueWithThisStopInThisAdministrativeAreaRef = true;
                                break;

                            default:
                                continueWithThisStopInThisAdministrativeAreaRef = false;
                                break;
                        }

                        break;

                    case "147":
                        stopType = "tram_stop";
                        default_method_accuracy = 20;

                            // Don't add anything other than each actual stop platform from
                            // the tram area as to keep each side seperate which is preferred
                            // for consistency and data reasons.
                        
                        switch(stop_type_code) {
                            case "PLT":
                                continueWithThisStopInThisAdministrativeAreaRef = true;
                                break;

                            default:
                                continueWithThisStopInThisAdministrativeAreaRef = false;
                                break;
                        }

                        break;

                    default:
                        stopType = "bus_stop";
                        default_method_accuracy = 20;

                            // Don't add anything other than actual bus stops from the regional
                            // areas as tram and train nodes have their own national collection
                            // which is preferred for consistency and data reasons.

                        switch(stop_type_code) {
                            case "BCT":
                                continueWithThisStopInThisAdministrativeAreaRef = true;
                                break;

                            case "BCS":
                                continueWithThisStopInThisAdministrativeAreaRef = true;
                                break;

                            case "BCQ":
                                continueWithThisStopInThisAdministrativeAreaRef = true;
                                break;

                            default:
                                continueWithThisStopInThisAdministrativeAreaRef = false;
                                break;
                        }

                        break;
                }

                if(continueWithThisStopInThisAdministrativeAreaRef) {
                    let atco_code = stopPoint['AtcoCode'] ? stopPoint['AtcoCode'][0] : null;

                    let stopTypeCodeKeys = {
                        "BCT": {
                            "position": "OnStreet",
                            "mode": "Bus",
                            "types" : [
                                "MarkedPoint",
                                "UnmarkedPoint",
                                "HailAndRide",
                                "FlexibleZone"
                            ]
                        },
                        "BCS": {
                            "position": "OffStreet",
                            "mode": "BusAndCoach",
                            "types" : [
                                "Bay"
                            ]
                        },
                        "BCQ": {
                            "position": "OffStreet",
                            "mode": "Bus",
                            "types" : [
                                "VariableBay"
                            ]
                        },
                        "PLT": {
                            "position": "OffStreet",
                            "mode": "Metro",
                            "types" : [
                                "Platform"
                            ]
                        },
                        "RLY": {
                            "position": "OffStreet",
                            "mode": "Rail",
                            "types" : [
                                "AccessArea"
                            ]
                        }
                    };

                    let stopObject = {}

                    stopObject['atco_code'] = atco_code ? atco_code : null;
                    stopObject['naptan_code'] = stopPoint['NaptanCode'] ? stopPoint['NaptanCode'][0] : null;
                    stopObject['plate_code'] = stopPoint['PlateCode'] ? stopPoint['PlateCode'][0] : null;

                    let key1 = stopTypeCodeKeys[stop_type_code]['position'];
                    let key2 = stopTypeCodeKeys[stop_type_code]['mode'];
                    let key3;

                    try {
                        for(let type in stopTypeCodeKeys[stop_type_code]['types']) {
                            type = stopTypeCodeKeys[stop_type_code]['types'][type];
                            if(!key3) {
                                for(let key in Object.keys(stopPoint['StopClassification'][0][key1][0][key2][0])) {
                                    key = Object.keys(stopPoint['StopClassification'][0][key1][0][key2][0])[key];
                                    if(!key3) {
                                        type === key ? key3 = key : null;
                                    }
                                }
                            }
                        }
                    } catch(e) {
                        console.warn(e);
                        throw new Error(e);
                    }

                    stopObject['tiploc_code'] = stopType === "train_station" && stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'] && stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'][0] ? stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'][0]['TiplocRef'][0] : null;
                    stopObject['crs_code'] = stopType === "train_station" && stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'] && stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'][0] ? stopPoint['StopClassification'][0][key1][0][key2][0]['AnnotatedRailRef'][0]['CrsRef'][0] : null;

                    stopObject['type'] = stopType;
                    stopObject['type_code'] = stop_type_code;
                    stopObject['position_type'] = key1 ? key1 : null;
                    stopObject['vehicle_type'] = key2 ? key2 : null;
                    stopObject['marking_type'] = key3 ? key3 : null;

                    if(!key1 && !key2 && !key3) {
                        console.log("A key is null for", stopObject['atco_code'], ":", key1, key2, key3);
                        return;
                    }

                    stopObject['name'] = stopPoint['Descriptor'][0]['CommonName'] ? stopPoint['Descriptor'][0]['CommonName'][0] : null;
                    stopObject['short_name'] = stopPoint['Descriptor'][0]['ShortCommonName'] ? stopPoint['Descriptor'][0]['ShortCommonName'][0] : null;
                    stopObject['description'] = null;
                    stopObject['indicator'] = stopPoint['Descriptor'][0]['Indicator'] ? stopPoint['Descriptor'][0]['Indicator'][0] : null;

                    stopObject['street_name'] = stopPoint['Descriptor'][0]['Street'] ? stopPoint['Descriptor'][0]['Street'][0] : null;
                    stopObject['crossing_name'] = stopPoint['Descriptor'][0]['Crossing'] ? stopPoint['Descriptor'][0]['Crossing'][0] : null;
                    stopObject['landmark_name'] = stopPoint['Descriptor'][0]['Landmark'] ? stopPoint['Descriptor'][0]['Landmark'][0] : null;

                    stopObject['plusbus_zone'] = stopPoint['PlusbusZones'] ? stopPoint['PlusbusZones'][0]['PlusbusZoneRef'] ? stopPoint['PlusbusZones'][0]['PlusbusZoneRef'][0]['$']['Status'] === 'active' ? stopPoint['PlusbusZones'][0]['PlusbusZoneRef'][0]['_'] : null : null : null;
                    stopObject['administrative_area_reference'] = stopPoint['AdministrativeAreaRef'] ? stopPoint['AdministrativeAreaRef'][0] : null;
                    stopObject['nptg_locality_reference'] = stopPoint['Place'][0]['NptgLocalityRef'] ? stopPoint['Place'][0]['NptgLocalityRef'][0] : null;

                    stopObject['latitude'] = stopPoint['Place'][0]['Location'][0]['Translation'][0]['Latitude'] ? stopPoint['Place'][0]['Location'][0]['Translation'][0]['Latitude'][0] : null;
                    stopObject['longitude'] = stopPoint['Place'][0]['Location'][0]['Translation'][0]['Longitude'] ? stopPoint['Place'][0]['Location'][0]['Translation'][0]['Longitude'][0] : null;
                    stopObject['accuracy'] = default_method_accuracy;
                    stopObject['geohash'] = ngeohash.encode(stopObject['latitude'], stopObject['longitude']);

                    stopObject['compass_point'] = stopPoint['StopClassification'][0][key1][0][key2][0] && stopPoint['StopClassification'][0][key1][0][key2][0][key3] && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0] && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing']  && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]  && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]['CompassPoint'] ? stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]['CompassPoint'][0] : null;
                    stopObject['compass_degrees'] = stopPoint['StopClassification'][0][key1][0][key2][0] && stopPoint['StopClassification'][0][key1][0][key2][0][key3] && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0] && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing']  && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]  && stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]['Degrees'] ? stopPoint['StopClassification'][0][key1][0][key2][0][key3][0]['Bearing'][0]['Degrees'][0] : null;

                    stopObject['created'] = currentTimestamp;
                    stopObject['last_modified'] = currentTimestamp;

                    let stopArray = [];

                    for(let item in stopObject) {
                        item = stopObject[item];
                        stopArray.push(item);
                    }

                    outputObject['stops'] ? null : outputObject['stops'] = [];
                    outputObject['stops'].push(stopArray);
                }
            }
        }
        console.log("Completed:", file, "B¬)");
    };

    let pool = mariadb.createPool({host: process.env.databaseHost, user: process.env.databaseUser, password: process.env.databasePassword, database: process.env.databaseName, connectionLimit: 35});
    let conn;

    try {
        console.log("Opening connection with DB...");

        conn = await pool.getConnection();

        conn.beginTransaction();

        console.log("Opened connection with DB!");

        try {
            console.log("Adding data to DB...");

            await conn.batch(`INSERT INTO
                stops(atco_code, naptan_code, plate_code, tiploc_code, crs_code, type, type_code, position_type, vehicle_type, marking_type, name, short_name, description, indicator, street_name, crossing_name, landmark_name, plusbus_zone, administrative_area_reference, nptg_locality_reference, latitude, longitude, accuracy, geohash, compass_point, compass_degrees, created, last_modified)
                VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY
                UPDATE
                naptan_code = VALUES(naptan_code), plate_code = VALUES(plate_code), tiploc_code = VALUES(tiploc_code), crs_code = VALUES(crs_code),type = VALUES(type), type_code = VALUES(type_code), position_type = VALUES(position_type), vehicle_type = VALUES(vehicle_type), marking_type = VALUES(marking_type), name = VALUES(name), short_name = VALUES(short_name), description = VALUES(description), indicator = VALUES(indicator), street_name = VALUES(street_name), crossing_name = VALUES(crossing_name), landmark_name = VALUES(landmark_name), plusbus_zone = VALUES(plusbus_zone), administrative_area_reference = VALUES(administrative_area_reference), nptg_locality_reference = VALUES(nptg_locality_reference), latitude = VALUES(latitude), longitude = VALUES(longitude), accuracy = VALUES(accuracy), geohash = VALUES(geohash), compass_point = VALUES(compass_point), compass_degrees = VALUES(compass_degrees), last_modified = VALUES(last_modified)`, outputObject['stops']);
            
            console.log("Comitting data to DB...");
            conn.commit();

            console.log("Successfully pushed stops to DB!");
        } catch(e) {
            console.log("Rolling back changes from DB...");
            conn.rollback();
            console.log("Error pushing stops to DB...");
            console.error(e);
        }
    } catch (e) {
        console.log("Error with DB...");
        console.error(e);
    } finally {
        console.log("Closing connection with DB...");
        conn ? conn.release() : null;
        pool ? pool.end() : null;
        console.log("Closed connection with DB!");
    }

    fs.writeFileSync(`Naptan.json`, JSON.stringify(outputObject, null, 4));

    return;
}

run().then(() => {
    console.log("Script complete!");
}).catch((e) => {
    console.log("Script Error...")
    console.log(e);
});