import { useState, useRef, useEffect } from "react";
import { select, scaleLinear, min, max, axisBottom, axisLeft } from "d3";
import { Dims } from "../../utils.js";
import { STATS, formatStat } from "./defs.js";

const dims = new Dims(600, 400, 40, 20, 50, 20);

const fetchTooltipData = async () => {
    const response = await fetch(`/data/standard_batting/tooltip.json`);
    return response.json();
};

const StatTooltip = ({ obj, stat, x, y }) => {
    const [tooltipData, setTooltipData] = useState(null);
    const svgRef = useRef(null);
    const isVisible = obj && stat;

    useEffect(() => {
        const get = async () => {
            const data = await fetchTooltipData();
            setTooltipData(data);
        };
        get();
    }, []);

    useEffect(() => {
        if (!(tooltipData && obj && stat)) return;

        const svg = select(svgRef.current)
            .attr("width", dims.width)
            .attr("height", dims.height)
            .style("font-family", "Roboto");

        const data = tooltipData[stat][obj.season];
        const val = obj[stat];
        const percVal = obj[`${stat}_perc`];
        const statLabel = STATS[stat];

        svg.selectAll("*").remove();

        const cont = svg.append("g").attr("transform", dims.containerTransform);
        const xAxisG = cont.append("g").attr("transform", dims.bottomAxisTransform);
        const yAxisG = cont.append("g");

        const xScaler = scaleLinear()
            .domain([min(data, (d) => d.bin_left), max(data, (d) => d.bin_right)])
            .range([0, dims.innerWidth]);

        const yScaler = scaleLinear()
            .domain([0, max(data, (d) => d.count)])
            .range([dims.innerHeight, 0]);

        yAxisG.call(axisLeft(yScaler));
        xAxisG.call(axisBottom(xScaler));

        // histogram
        cont.selectAll("rect")
            .data(data)
            .enter()
            .append("rect")
            .attr("x", (d, i) => xScaler(d.bin_left))
            .attr("y", (d) => yScaler(d.count))
            .attr("width", (d) => xScaler(d.bin_right) - xScaler(d.bin_left))
            .attr("height", (d) => dims.innerHeight - yScaler(d.count))
            .attr("fill", "#6200EE")
            .attr("stroke", "lightgray")
            .attr("stroke-width", 0.5);

        // vertical annotation line at player's value
        cont.append("line")
            .attr("x1", xScaler(val))
            .attr("x2", xScaler(val))
            .attr("y1", 0)
            .attr("y2", dims.innerHeight)
            .attr("stroke", "black")
            .attr("stroke-dasharray", "4 1")
            .attr("stroke-width", 1);

        // label for player's value/percentile at the annotation line
        const gap = 10; // between label and annotation line
        const px = 5; // padding for the rectangle around the label
        const py = 5;

        const lab = cont
            .append("text")
            .attr("x", xScaler(val) + gap)
            .attr("y", 20)
            .attr("font-size", 12)
            .text(`${formatStat(obj, stat)} (${percVal})`);

        // determine if the label needs to be flipped to the other side of the annotation line
        let bbox = lab.node().getBBox();
        if (bbox.x + bbox.width + px >= dims.innerWidth) {
            lab.attr("x", xScaler(val) - gap);
            lab.attr("text-anchor", "end");
            bbox = lab.node().getBBox();
        }

        cont.append("rect")
            .attr("x", bbox.x - px)
            .attr("y", bbox.y - px)
            .attr("width", bbox.width + 2 * px)
            .attr("height", bbox.height + 2 * py)
            .attr("fill", "#ddd")
            .attr("stroke", "black")
            .attr("stroke-width", 0.5);

        lab.raise(); // rectangle drawn on top of the text, fix

        // chart title
        svg.append("text")
            .attr("x", dims.innerWidth / 2)
            .attr("y", dims.marginTop / 2)
            .attr("text-anchor", "middle")
            .text(`${statLabel} (${stat}) - ${obj.season} Season`);
    }, [tooltipData, obj, stat]);

    return (
        <div
            className={`absolute bg-white border border-gray-300 p-2 rounded shadow-lg pointer-events-none ${
                isVisible ? "block" : "hidden"
            }`}
            style={{
                left: x > window.innerWidth / 2 ? x - dims.width - 50 : x + 20,
                top: y > window.innerHeight / 2 ? y - dims.height : y,
            }}
        >
            <svg ref={svgRef}></svg>
        </div>
    );
};

export default StatTooltip;
