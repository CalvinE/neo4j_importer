import fs from "fs";
import path from "path";
import os from "os";

import neo4j, { Session } from "neo4j-driver";

const FIELD_SEPERATOR = ",";
const EOL = os.EOL;
const SOURCE = "realtor.com";
const ADDITIONAL_PROPERTIES = {
    source: SOURCE,
};
const DEFAULT_LABEL = "RAW_PROPERTY_SALE_INFO";

const addNodeIfNotPresent = async(session: Session,
                                  data: any,
                                  addProcessedFlag: boolean = false,
                                  labelToUseForNewEntities: string,
                                  uniqueIdentifierFieldName?: string) => {

    const clauses: string[] = [];
    const keys = Object.keys(data);
    if (uniqueIdentifierFieldName && data.hasOwnProperty(uniqueIdentifierFieldName)) {
        clauses.push(`node.${uniqueIdentifierFieldName} = ${JSON.stringify(data[uniqueIdentifierFieldName])}`);
    } else {
        keys.forEach((key) => {
            clauses.push(`node.${key} = ${JSON.stringify(data[key])}`)
        });
    }

    const joinedClause = clauses.join(" AND ");
    const query = `\
        MATCH (node:${DEFAULT_LABEL}) \
        WHERE ${joinedClause} \
        RETURN node`;
    const result = await session.run(query);
    if (result.records.length === 0) {
        if (addProcessedFlag) {
            data.processed = false;
        }
        const props: string[] = [];
        keys.forEach((key) => {
            props.push(`${key}: ${JSON.stringify(data[key])}`);
        });
        const joinedProps = props.join(", ");
        // insert
        const inertQuery = `\
        CREATE (node:${DEFAULT_LABEL}{${joinedProps}}) \
        RETURN node`;
        await session.run(inertQuery);
    } // else {
    //     console.log("skipping item because it exists", data);
    // }
}

const csvToJSON = (targetContents: string) => {
    const data: any[] = [];
    const keys: string[] = [];
    const targetLines = targetContents.split(EOL);
    if (targetLines.length > 1) {
        const headers = targetLines[0].split(FIELD_SEPERATOR);
        for (const key of headers) {
            keys.push(key);
        }
        if (keys.length > 0) {
            for (let i = 1; i < targetLines.length; i++) {
                let hasValue = false;
                const fields = targetLines[i].split(FIELD_SEPERATOR);
                const dataItem: any = {};
                keys.forEach((key, index) => {
                    const value = fields[index];
                    if (value !== undefined && value !== null && value !== "") {
                        dataItem[key] = value;
                        hasValue = true;
                    }
                });
                if (hasValue) {
                    data.push(dataItem);
                }
            }
        }
    }
    return data;
}

const run = async () => {
    const typeIndex = process.argv.indexOf("--type") + 1;
    const pathIndex = process.argv.indexOf("--path") + 1;
    const subPropertyIndex = process.argv.indexOf("--subProperty") + 1;
    const addProcessedFlag = process.argv.indexOf("--addProcessedFlag") !== -1;
    const uniqueIdentifierFieldIndex = process.argv.indexOf("--uniqueIdentifierField") + 1;
    const newEntityLabelIndex = process.argv.indexOf("--newEntityLabel") + 1;

    if (typeIndex === 0) {
        throw new Error("--type was not provided!");
    }

    if (pathIndex === 0) {
        throw new Error("--path was not provided!");
    }

    const targetPath = process.argv[pathIndex];
    const fileType = process.argv[typeIndex];

    if (fileType !== "csv" && fileType !== "json") {
        throw new Error("--type must be either 'csv' or 'json'");
    }

    const uniqueIdentifierFieldName = uniqueIdentifierFieldIndex > 0 ? process.argv[uniqueIdentifierFieldIndex] : undefined;

    const pathContents = fs.readdirSync(process.argv[pathIndex]).filter((f) => f.toLowerCase().endsWith(fileType === "csv" ? ".csv" : ".json"));

    const newEntityLabel = newEntityLabelIndex > 0 ? process.argv[newEntityLabelIndex] : DEFAULT_LABEL;

    const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("", ""), {

    });

    const session = driver.session();

    for (const file of pathContents) {
        const target = path.join(targetPath, file);
        const parseLabel = `Parsing contents from file ${target}`;
        console.time(parseLabel);
        const targetContents = fs.readFileSync(target, {
            encoding: "utf8"
        });
        let data: any[];

        if  (fileType === "csv") {
            data = csvToJSON(targetContents);
        } else {
            data = JSON.parse(targetContents);
            if (subPropertyIndex > 0) {
                data = data.map((item) => item[process.argv[subPropertyIndex]]);
            }
        }
        console.timeEnd(parseLabel);
        const graphLabel = `${data.length} records processed from ${target}`;
        console.time(graphLabel);

        if (ADDITIONAL_PROPERTIES) {
            data = data.map((item) => {
                return {
                    ...ADDITIONAL_PROPERTIES,
                    ...item,
                }
            });
        }

        for (const item of data) {
            await addNodeIfNotPresent(session, item, addProcessedFlag, newEntityLabel, uniqueIdentifierFieldName);
        }

        console.timeEnd(graphLabel);
    }
};

run();