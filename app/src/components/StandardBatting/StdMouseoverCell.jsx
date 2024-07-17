import { formatStat } from "./defs";



const StdMouseoverCell = ({
    obj,
    stat,
    mouseoverHandler,
    mouseoutHandler,
}) => {
    return (
        <td
            className="py-3 px-4 text-left cursor-pointer"
            onMouseEnter={(event) => mouseoverHandler(event, obj, stat)}
            onMouseLeave={mouseoutHandler}
        >
            {formatStat(obj, stat)}
        </td>
    );
};

export default StdMouseoverCell;
