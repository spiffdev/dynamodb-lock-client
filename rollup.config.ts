import typescript from "@rollup/plugin-typescript";
import { exec } from "child_process";
import { defineConfig, Plugin } from "rollup";
import json from "@rollup/plugin-json";

/**
 * We implement type generation using tsup, it efficiently generates types
 * into a single rolled up file.
 */
const dts = (): Plugin => ({
    name: "dts",
    buildEnd: (error) => {
        if (error) {
            return;
        }
        exec("tsup index.ts --format esm --dts-only", (err) => {
            if (err) {
                throw new Error(`Failed to generate types: ${err.message}`);
            }
        });
    },
});

export default defineConfig({
    input: "./index.ts",
    plugins: [
        typescript({
            tsconfig: "./tsconfig.json",
        }),
        json(),
        dts(),
    ],
    output: {
        file: "dist/index.mjs",
        format: "esm",
    },
});
