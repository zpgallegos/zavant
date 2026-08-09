const StdValuesRow = ({ obj, children }) => {
    return (
        <tr
            className={
                `border-b border-gray-200 hover:bg-secondary ` +
                (obj.team_short === "TOT"
                    ? "bg-gray-300"
                    : obj.complete
                    ? ""
                    : "bg-gray-200")
            }
        >
            {children}
        </tr>
    );
};

export default StdValuesRow;
