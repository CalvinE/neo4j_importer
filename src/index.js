"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const os_1 = __importDefault(require("os"));
const neo4j_driver_1 = __importDefault(require("neo4j-driver"));
const FIELD_SEPERATOR = ",";
const EOL = os_1.default.EOL;
const SOURCE = "realtor.com";
const ADDITIONAL_PROPERTIES = {
    source: SOURCE,
};
const DEFAULT_LABEL = "RAW_PROPERTY_SALE_INFO";
const addNodeIfNotPresent = (session, data, addProcessedFlag = false, labelToUseForNewEntities, uniqueIdentifierFieldName) => __awaiter(void 0, void 0, void 0, function* () {
    const clauses = [];
    const keys = Object.keys(data);
    if (uniqueIdentifierFieldName && data.hasOwnProperty(uniqueIdentifierFieldName)) {
        clauses.push(`node.${uniqueIdentifierFieldName} = ${JSON.stringify(data[uniqueIdentifierFieldName])}`);
    }
    else {
        keys.forEach((key) => {
            clauses.push(`node.${key} = ${JSON.stringify(data[key])}`);
        });
    }
    const joinedClause = clauses.join(" AND ");
    const query = `\
        MATCH (node:${DEFAULT_LABEL}) \
        WHERE ${joinedClause} \
        RETURN node`;
    const result = yield session.run(query);
    if (result.records.length === 0) {
        if (addProcessedFlag) {
            data.processed = false;
        }
        const props = [];
        keys.forEach((key) => {
            props.push(`${key}: ${JSON.stringify(data[key])}`);
        });
        const joinedProps = props.join(", ");
        // insert
        const inertQuery = `\
        CREATE (node:${DEFAULT_LABEL}{${joinedProps}}) \
        RETURN node`;
        yield session.run(inertQuery);
    } // else {
    //     console.log("skipping item because it exists", data);
    // }
});
const csvToJSON = (targetContents) => {
    const data = [];
    const keys = [];
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
                const dataItem = {};
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
};
const run = () => __awaiter(void 0, void 0, void 0, function* () {
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
    const pathContents = fs_1.default.readdirSync(process.argv[pathIndex]).filter((f) => f.toLowerCase().endsWith(fileType === "csv" ? ".csv" : ".json"));
    const newEntityLabel = newEntityLabelIndex > 0 ? process.argv[newEntityLabelIndex] : DEFAULT_LABEL;
    const driver = neo4j_driver_1.default.driver("bolt://localhost:7687", neo4j_driver_1.default.auth.basic("", ""), {});
    const session = driver.session();
    for (const file of pathContents) {
        const target = path_1.default.join(targetPath, file);
        const parseLabel = `Parsing contents from file ${target}`;
        console.time(parseLabel);
        const targetContents = fs_1.default.readFileSync(target, {
            encoding: "utf8"
        });
        let data;
        if (fileType === "csv") {
            data = csvToJSON(targetContents);
        }
        else {
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
                return Object.assign(Object.assign({}, ADDITIONAL_PROPERTIES), item);
            });
        }
        for (const item of data) {
            yield addNodeIfNotPresent(session, item, addProcessedFlag, newEntityLabel, uniqueIdentifierFieldName);
        }
        console.timeEnd(graphLabel);
    }
});
run();
//# sourceMappingURL=index.js.map