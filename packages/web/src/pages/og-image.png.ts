import type { APIRoute } from "astro";
import { createElement as h } from "react";
import satori from "satori";
import { Resvg } from "@resvg/resvg-js";
import { readFile } from "node:fs/promises";

async function loadFont(
  family: string,
  weight: number,
): Promise<ArrayBuffer> {
  const css = await fetch(
    `https://fonts.googleapis.com/css2?family=${family.replace(/ /g, "+")}:wght@${weight}`,
    { headers: { "User-Agent": "Mozilla/4.0" } },
  ).then((r) => r.text());
  const url = css.match(/src:\s*url\(([^)]+\.ttf)\)/)?.[1];
  if (!url) throw new Error(`Font not found: ${family}:${weight}`);
  return fetch(url).then((r) => r.arrayBuffer());
}

export const GET: APIRoute = async () => {
  const [regular, bold, icon] = await Promise.all([
    loadFont("Plus Jakarta Sans", 400),
    loadFont("Plus Jakarta Sans", 800),
    readFile("public/service-icon.png"),
  ]);

  const iconSrc = `data:image/png;base64,${icon.toString("base64")}`;

  const svg = await satori(
    h(
      "div",
      {
        style: {
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          background:
            "radial-gradient(ellipse at 50% 40%, #13132a, #09090b 70%)",
          fontFamily: "Plus Jakarta Sans",
        },
      },
      h("img", {
        src: iconSrc,
        width: 104,
        height: 104,
        style: { marginBottom: 24 },
      }),
      h(
        "div",
        {
          style: {
            display: "flex",
            fontSize: 72,
            fontWeight: 800,
            letterSpacing: "-0.04em",
            lineHeight: 1.15,
            marginBottom: 16,
          },
        },
        h("span", { style: { color: "#f4f4f5" } }, "The database"),
        h("span", { style: { color: "#818cf8", marginLeft: 10 } }, "of your life"),
      ),
      h(
        "div",
        {
          style: {
            fontSize: 28,
            fontWeight: 400,
            color: "#a1a1aa",
            marginBottom: 24,
          },
        },
        "One SQLite file. AI structures everything. You just talk.",
      ),
      h(
        "div",
        {
          style: {
            fontSize: 22,
            fontWeight: 500,
            color: "#52525b",
            letterSpacing: "0.04em",
          },
        },
        "getlore.sh",
      ),
    ),
    {
      width: 1200,
      height: 630,
      fonts: [
        { name: "Plus Jakarta Sans", data: regular, weight: 400 as const, style: "normal" as const },
        { name: "Plus Jakarta Sans", data: bold, weight: 800 as const, style: "normal" as const },
      ],
    },
  );

  const resvg = new Resvg(svg, {
    fitTo: { mode: "width" as const, value: 1200 },
  });
  const png = resvg.render().asPng();

  return new Response(new Uint8Array(png), {
    headers: { "Content-Type": "image/png" },
  });
};
