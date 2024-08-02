import { useRef, useEffect, useState } from "react";
import { select, hierarchy, partition, arc, scaleOrdinal, schemeTableau10 } from "d3";
import { Dims } from "../../utils";

const AtBatSunburst = ({ data }) => {
    const [points, setPoints] = useState([]);

    const breadcrumbSvgRef = useRef(null);
    const svgRef = useRef(null);

    useEffect(() => {
        const dims = new Dims(800, 800, 20, 20, 20, 20);
        const radius = dims.width / 2;

        const tipWidth = 10;
        const breadcrumbWidth = 75;
        const breadcrumbHeight = 30;

        const getBreadcrumbPath = (d, i) => {
            const points = [];
            points.push("0,0");
            points.push(`${breadcrumbWidth},0`);
            points.push(`${breadcrumbWidth + tipWidth},${breadcrumbHeight / 2}`);
            points.push(`${breadcrumbWidth},${breadcrumbHeight}`);
            points.push(`0,${breadcrumbHeight}`);
            if (i > 0) {
                points.push(`${tipWidth},${breadcrumbHeight / 2}`);
            }
            return points.join(" ");
        };

        const root = partition().size([2 * Math.PI, radius * radius])(
            hierarchy(data)
                .sum((d) => d.value)
                .sort((a, b) => b.value - a.value)
        );

        const filtered = root.descendants().filter((d) => d.depth && d.x1 - d.x0 > 0.01);

        const colorScaler = scaleOrdinal()
            .domain(filtered.map((d) => d.data.name))
            .range(schemeTableau10);

        const arcGenerator = arc()
            .startAngle((d) => d.x0)
            .endAngle((d) => d.x1)
            .innerRadius((d) => Math.sqrt(d.y0))
            .outerRadius(radius);

        const mouseArc = arc()
            .startAngle((d) => d.x0)
            .endAngle((d) => d.x1)
            .innerRadius((d) => Math.sqrt(d.y0))
            .outerRadius(radius);

        const svg = select(svgRef.current)
            .attr("width", dims.width)
            .attr("height", dims.height)
            .attr("viewBox", [-radius, -radius, dims.width, dims.height])
            .style("font-family", "Roboto");

        const element = svg.node();
        element.value = { sequence: [], percentage: 0.0 };

        const bSvg = select(breadcrumbSvgRef.current)
            .attr("viewBox", [0, 0, breadcrumbWidth * 10, breadcrumbHeight])
            .style("font", "12px sans-serif")
            .style("margin", "5px");

        const path = svg
            .append("g")
            .selectAll("path")
            .data(filtered)
            .join("path")
            .attr("d", arcGenerator)
            .attr("stroke", "black")
            .attr("fill", (d) => colorScaler(d.data.name));

        const label = svg
            .append("text")
            .attr("text-anchor", "middle")
            .attr("fill", "#888")
            .style("visibility", "hidden");

        label
            .append("tspan")
            .attr("class", "percentage")
            .attr("x", 0)
            .attr("y", 0)
            .attr("dy", "-0.1em")
            .attr("font-size", "3em")
            .text("");

        label
            .append("tspan")
            .attr("x", 0)
            .attr("y", 0)
            .attr("dy", "1.5em")
            .text("of at-bats begin with this sequence");

        svg.append("g")
            .attr("class", "sequence")
            .attr("fill", "none")
            .attr("pointer-events", "all")
            .on("mouseleave", () => {
                path.attr("fill-opacity", 1);
                label.style("visibility", "hidden");
                element.value = { sequence: [], percentage: 0.0 };
                element.dispatchEvent(new CustomEvent("input"));
                bSvg.selectAll("*").remove();
            })
            .selectAll("path")
            .data(filtered)
            .join("path")
            .attr("d", mouseArc)
            .on("mouseenter", (event, d) => {
                bSvg.selectAll("*").remove();

                const sequence = d.ancestors().reverse().slice(1);
                path.attr("fill-opacity", (node) =>
                    sequence.indexOf(node) >= 0 ? 1.0 : 0.3
                );
                const percentage = ((100 * d.value) / root.value).toPrecision(3);

                label
                    .style("visibility", null)
                    .select(".percentage")
                    .text(percentage + "%");
                element.value = { sequence, percentage };
                element.dispatchEvent(new CustomEvent("input"));

                const bg = bSvg
                    .selectAll("g")
                    .data(sequence)
                    .join("g")
                    .attr("transform", (d, i) => `translate(${i * breadcrumbWidth}, 0)`);

                bg.append("polygon")
                    .attr("points", getBreadcrumbPath)
                    .attr("fill", (d) => colorScaler(d.data.name))
                    .attr("stroke", "white");

                bg.append("text")
                    .attr("x", (breadcrumbWidth + 10) / 2)
                    .attr("y", 15)
                    .attr("dy", "0.35em")
                    .attr("text-anchor", "middle")
                    .attr("fill", "white")
                    .text((d) => d.data.name);

                bSvg.append("text")
                    .text(percentage > 0 ? percentage + "%" : "")
                    .attr("x", (sequence.length + 0.5) * breadcrumbWidth)
                    .attr("y", breadcrumbHeight / 2)
                    .attr("dy", "0.35em")
                    .attr("text-anchor", "middle")
                    .attr("fill", "white");
            });
    }, [data]);

    return (
        <div className=" bg-white shadow-lg rounded-lg overflow-hidden">
            <div className="bg-gray-800 p-4">
                <h2 className="text-white text-2xl font-semibold">AB Outcomes</h2>
            </div>
            <div>
                <div className="p-4 mx-auto">
                    <svg ref={breadcrumbSvgRef}></svg>
                    <svg ref={svgRef}></svg>
                </div>
            </div>
        </div>
    );
};

export default AtBatSunburst;
