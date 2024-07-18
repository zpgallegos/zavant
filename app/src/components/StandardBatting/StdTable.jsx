import { useState } from "react";
import StdValueTooltip from "./StdValueTooltip";
import StdHeaderCell from "./StdHeaderCell";
import StdValueCell from "./StdValueCell";
import StdMouseoverCell from "./StdMouseoverCell";
import StdValuesRow from "./StdValuesRow";
import { STATS } from "./defs";

const StdTable = ({ data }) => {
    const tooltipInitialState = { obj: null, stat: null };
    const [tooltipState, setTooltipState] = useState(tooltipInitialState);
    const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

    const handleMouseEnter = (event, obj, stat) => {
        if (!obj.complete) return;
        setTooltipState({ obj, stat });
        setTooltipPosition({ x: event.pageX + 10, y: event.pageY + 10 });
    };

    const handleMouseLeave = () => {
        setTooltipState(tooltipInitialState);
    };

    return (
        <div className=" bg-white shadow-lg rounded-lg overflow-hidden">
            <div className="bg-gray-800 p-4">
                <h2 className="text-white text-2xl font-semibold">Standard Batting</h2>
            </div>
            <div className="overflow-x-auto">
                <table className="min-w-full bg-white">
                    <thead>
                        <tr className="w-full bg-gray-400 text-gray-800 uppercase text-sm leading-normal">
                            <StdHeaderCell>Season</StdHeaderCell>
                            <StdHeaderCell>Team</StdHeaderCell>
                            <StdHeaderCell>League</StdHeaderCell>
                            <StdHeaderCell>G</StdHeaderCell>
                            <StdHeaderCell>PA</StdHeaderCell>
                            <StdHeaderCell>AB</StdHeaderCell>
                            <StdHeaderCell>R</StdHeaderCell>
                            <StdHeaderCell>H</StdHeaderCell>
                            <StdHeaderCell>2B</StdHeaderCell>
                            <StdHeaderCell>3B</StdHeaderCell>
                            <StdHeaderCell>HR</StdHeaderCell>
                            <StdHeaderCell>RBI</StdHeaderCell>
                            <StdHeaderCell>SB</StdHeaderCell>
                            <StdHeaderCell>CS</StdHeaderCell>
                            <StdHeaderCell>BB</StdHeaderCell>
                            <StdHeaderCell>SO</StdHeaderCell>
                            <StdHeaderCell>BA</StdHeaderCell>
                            <StdHeaderCell>OBP</StdHeaderCell>
                            <StdHeaderCell>SLG</StdHeaderCell>
                            <StdHeaderCell>OPS</StdHeaderCell>
                            <StdHeaderCell>TB</StdHeaderCell>
                            <StdHeaderCell>GIDP</StdHeaderCell>
                            <StdHeaderCell>HBP</StdHeaderCell>
                            <StdHeaderCell>SH</StdHeaderCell>
                            <StdHeaderCell>SF</StdHeaderCell>
                            <StdHeaderCell>IBB</StdHeaderCell>
                        </tr>
                    </thead>
                    <tbody className="text-gray-700 text-sm font-light">
                        {data.map((obj, index) => (
                            <StdValuesRow key={index} obj={obj}>
                                <StdValueCell>{obj.season}</StdValueCell>
                                <StdValueCell>{obj.team_short}</StdValueCell>
                                <StdValueCell>{obj.league_name_short}</StdValueCell>
                                {Object.keys(STATS).map((stat) => (
                                    <StdMouseoverCell
                                        key={stat}
                                        obj={obj}
                                        stat={stat}
                                        mouseoverHandler={handleMouseEnter}
                                        mouseoutHandler={handleMouseLeave}
                                    />
                                ))}
                            </StdValuesRow>
                        ))}
                    </tbody>
                </table>
            </div>
            <StdValueTooltip
                obj={tooltipState.obj}
                stat={tooltipState.stat}
                x={tooltipPosition.x}
                y={tooltipPosition.y}
            />
        </div>
    );
};

export default StdTable;
