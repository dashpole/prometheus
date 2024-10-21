import { escapeString } from "./escapeString";

export const formatSeries = (labels: { [key: string]: string }): string => {
  if (labels === null) {
    return "scalar";
  }

  return `${labels.__name__ || ""}{${Object.entries(labels)
    .filter(([k]) => k !== "__name__" && k !== "__type__" && k !== "__unit__")
    .map(([k, v]) => `${k}="${escapeString(v)}"`)
    .join(", ")}}`;
};
