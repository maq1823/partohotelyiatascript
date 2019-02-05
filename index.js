//parto static data url : https://cdn.partocrs.com/ApiDocument/StaticData/HotelStaticData.zip

const fs = require('fs').promises;
const arango = require('arangojs');
const aql = arango.aql;

const configs = require('./config').values;

const baseAddress = configs.staticFilesBaseAddress;

const db = new arango({
    url: configs.dbUrl
});
db.useDatabase(configs.dbName);
db.useBasicAuth(configs.dbUserName, configs.dbPassword);

//load a file content async
const readFileBody = function (fileAddress) {
    return fs.readFile(baseAddress + fileAddress, 'utf8');
}

const readFileBodyAsJSON = async function (fileAddress) {
    const fileContent = await readFileBody(fileAddress);
    return JSON.parse(fileContent);
}

const readFileNamesInDirectoriesFiltered = async function (dir, filter) {
    const fileNames = await fs.readdir(dir);
    return fileNames.filter(it => it.includes(filter));
}

//add new countries from Parto hotel to Country collection of DB
const addNewCountries = async function () {
    const hotelCountries = await readFileBodyAsJSON('Country.json');

    let flightCountries = [];
    const countryCollection = db.collection('Country');
    const cursor = await countryCollection.all();
    flightCountries = await cursor.all();

    const hotelDistinct = hotelCountries.filter(it => !flightCountries.some(ix => ix._key === it.Code));

    for (const hotelCountry of hotelDistinct) {
        const newCountry = {
            _key: hotelCountry.Code,
            CountryCode: hotelCountry.Code,
            CountryName: hotelCountry.Name
        };
        await countryCollection.save(newCountry);
    }

    console.info(`${hotelDistinct.length} countries added to Country collection`);
}

//create PartoHotelCity collection and add static data to it.
const addCityData = async function () {
    //if PartoCity collection doesn't exist, create it
    const partoCityCollection = db.collection('PartoHotelCity');
    if (await partoCityCollection.exists() === false) {
        await partoCityCollection.create();
        console.info('PartoCityCollection created');
    }
    //truncate data of the collection
    await partoCityCollection.truncate();
    console.info('PartoCityCollection truncated');

    //if HotelLookup collection doesn't exist, create it
    const hotelLookupCollection = db.collection('HotelLookup');
    if (await hotelLookupCollection.exists() === false) {
        await hotelLookupCollection.create();
        console.info('HotelLookupCollection created');
    }
    //truncate data of the collection
    await hotelLookupCollection.truncate();
    console.info('HotelLookupCollection truncated');

    //load countries from DB
    const countryCollection = db.collection('Country');
    const countryCursor = await countryCollection.all();
    const countries = await countryCursor.all();
    //load destinations
    const destinations = await readFileBodyAsJSON('PropertyDestination.json');
    //load cities
    const cities = await readFileBodyAsJSON('PropertyCity.json');

    const defaultTopDestinations = ["Penang Island", "Istanbul", "Hong Kong Island", "Frankfurt am Main", "Dubai"];

    let cityArray = [];
    let hotelLookupArray = [];

    //create city
    for (const city of cities) {
        lookupId = lookupId + 1;
        const dest = destinations.find(it => it.Id === city.PropertyDestinationId);
        const country = countries.find(it => it._key === dest.CountryId);
        const dbCity = {
            _key: city.Id.toString(),
            Name: city.Name,
            Destination: dest.Name,
            CountryId: country._key,
            lookupKey: lookupId.toString()
        };
        cityArray.push(dbCity);

        const rate = defaultTopDestinations.some(it => it === city.Name) ? 1 : 0;
        const lookupData = {
            _key: lookupId.toString(),
            Type: 1, //City
            Name: `${city.Name}, ${country.CountryName}`,
            Fulltext: `${city.Name} ${dest.Name} ${country.CountryName} ${country._key}`,
            Rate: rate,
            Providers: [
                {
                    Type: 1, //Parto
                    Key: city.Id.toString()
                }
            ]
        };
        hotelLookupArray.push(lookupData);

        // console.info(`City ${city.Id}-${city.Name} parsed`);
    }
    console.info('Adding all cities to PartoHotelCity collection');
    await partoCityCollection.import(cityArray);
    console.info('Adding all cities to HotelLookup collection');
    await hotelLookupCollection.import(hotelLookupArray);
    console.log(`${cities.length} cities added to PartoHotelCity collection`);
}

//create PartoHotelFacility collection and add static data to it.
const addFacilityData = async function () {
    //if PartoHotelFacility collection doesn't exist, create it
    const PartoHotelFacilityCollection = db.collection('PartoHotelFacility');
    if (await PartoHotelFacilityCollection.exists() === false) {
        await PartoHotelFacilityCollection.create();
        console.info('PartoHotelFacilityCollection created');
    }
    //truncate data of the collection
    await PartoHotelFacilityCollection.truncate();
    console.info('PartoHotelFacilityCollection truncated');
    //load facility groups
    const facilityGroups = await readFileBodyAsJSON('FacilityGroup.json');
    //load facilities
    const facilities = await readFileBodyAsJSON('Facility.json');
    for (const facility of facilities) {
        const facilityGroup = facilityGroups.find(it => it.Id === facility.FacilityGroupId);
        const dbFacility = {
            _key: facility.Id.toString(),
            Name: facility.Name,
            Group: facilityGroup.Name
        }
        await PartoHotelFacilityCollection.save(dbFacility);
        // console.info(`Facility ${facility.Id}-${facility.Name} added to PartoHotelFacility Collection`);
    }
    console.log(`${facilities.length} facilities added to PartoHotelFacility collection`);
}

//load chains intersected with propertyChains, meaning load propertyChains
const getPropertyChainMap = async function () {
    const chains = await readFileBodyAsJSON('Chain.json');
    const propertyChains = await readFileBodyAsJSON('PropertyChain.json');
    const aggPropertyChains = new Map();
    propertyChains.forEach(it => {
        const chainName = chains.find(ix => ix.Id === it.ChainId).Name;
        if (aggPropertyChains.has(it.PropertyId)) {
            const arr = aggPropertyChains.get(it.PropertyId);
            arr.push(chainName);
            aggPropertyChains.set(it.PropertyId, arr);
        } else {
            aggPropertyChains.set(it.PropertyId, [chainName]);
        }
    });
    console.info(`Size of PropertyChain map: ${aggPropertyChains.size}`);
    return aggPropertyChains;
}

const getPropertyFacilities = async function () {
    const fileNames = await readFileNamesInDirectoriesFiltered(baseAddress, 'PropertyFacility_');
    const aggPropertyFacilities = new Map();
    for (const fileName of fileNames) {
        const propertFacility = await readFileBodyAsJSON(fileName);
        propertFacility.forEach(it => {
            if (aggPropertyFacilities.has(it.PropertyId)) {
                const arr = aggPropertyFacilities.get(it.PropertyId);
                arr.push(it.FacilityId);
                aggPropertyFacilities.set(it.PropertyId, arr);
            } else {
                aggPropertyFacilities.set(it.PropertyId, [it.FacilityId]);
            }
        });
    }
    console.info(`Size of PropertyFacility map: ${aggPropertyFacilities.size}`);
    return aggPropertyFacilities;
}

//create PartoHotel collection and add static data to it.
const addHotelData = async function () {
    //if PartoHotel collection doesn't exist, create it
    const PartoHotelCollection = db.collection('PartoHotel');
    if (await PartoHotelCollection.exists() === false) {
        await PartoHotelCollection.create();
        console.info('PartoHotelCollection created');
    }
    //truncate data of the collection
    await PartoHotelCollection.truncate();
    console.info('PartoHotelCollection truncated');
    //load accomodations
    const accommodations = await readFileBodyAsJSON('PropertyAccommodation.json');
    //load chains intersected with propertyChains, meaning load propertyChains
    const propertyChains = await getPropertyChainMap();
    //load propertyFacilities from all those files
    const propertyFacilities = await getPropertyFacilities();
    //load countries from DB
    const countryCollection = db.collection('Country');
    const countryCursor = await countryCollection.all();
    const countries = await countryCursor.all();

    const partoCityCollection = db.collection('PartoHotelCity');
    const hotelLookupCollection = db.collection('HotelLookup');

    //load hotel and add them to db
    const hotelFileNames = await readFileNamesInDirectoriesFiltered(baseAddress, 'Property_');

    let hotelArray = [];
    let hotelLookupArray = [];

    for (const hotelFileName of hotelFileNames) {
        const hotels = await readFileBodyAsJSON(hotelFileName);

        hotelArray = [];
        hotelLookupArray = [];

        console.info(hotels.length + ' hotels in file: ' + hotelFileName);
        for (const hotel of hotels) {
            lookupId = lookupId + 1;
            hotel.lookupKey = lookupId.toString();
            hotel.CityId = hotel.PropertyCityId.toString();
            delete hotel.PropertyCityId;
            hotel.Accommodation = accommodations.find(ix => ix.Id === hotel.Accommodation).Name;
            hotel.Chains = propertyChains.get(hotel.Id);
            hotel.Facilities = propertyFacilities.get(hotel.Id) || [];
            hotel._key = hotel.Id.toString();
            delete hotel.Id;

            hotelArray.push(hotel);

            const cityCursor = await db.query(aql`FOR pc IN ${partoCityCollection} FILTER pc._key == ${hotel.CityId} LIMIT 1 RETURN pc`);
            const city = await cityCursor.next();
            const country = countries.find(it => it._key === city.CountryId);

            const lookupData = {
                _key: lookupId.toString(),
                Type: 2, //Hotel
                Name: `${hotel.Name}, ${city.Name} ${country.CountryName}`,
                Fulltext: `${hotel.Name} ${city.Name} ${city.Destination} ${country.CountryName} ${country._key}`,
                Rate: 0,
                Providers: [
                    {
                        Type: 1, //Parto
                        Key: hotel._key
                    }
                ]
            };
            hotelLookupArray.push(lookupData);
            // console.info(`Hotel ${hotel._key}-${hotel.Name} parsed`);
        }
        console.info('Adding one file of hotels to PartoHotel collection');
        await PartoHotelCollection.import(hotelArray);
        console.info('Adding one file of hotels to HotelLookup collection');
        await hotelLookupCollection.import(hotelLookupArray);
    }

    // create fulltext index
    console.info('ensure Fulltext index on HotelLookup');
    await hotelLookupCollection.createFulltextIndex("Fulltext");
}

let lookupId = 1;

const start = async function () {
    const start = Date.now();
    console.log('ADDING COUNTRIES IN-PROGRESS');
    await addNewCountries();
    console.log('ADDING COUNTRIES FINISHED');

    console.log('ADDING FACILITIES IN-PROGRESS');
    await addFacilityData();
    console.log('ADDING FACILITIES FINISHED');

    console.log('ADDING CITIES IN-PROGRESS');
    await addCityData();
    console.log('ADDING CITIES FINISHED');

    console.log('ADDING HOTELS IN-PROGRESS');
    await addHotelData();
    console.log('ADDING HOTELS FINISHED');

    console.info(`The whole script added ${lookupId} city/hotel to HotelLookup collection.`);
    console.info(`The whole script took ${(Date.now() - start) / 1000} seconds.`);
}

try {
    start();
} catch (error) {
    console.log('error in running script. It will likely be solved by providing a correct value in configs file. The actual error is : ', error);
}



