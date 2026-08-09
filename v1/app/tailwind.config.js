/** @type {import('tailwindcss').Config} */
module.exports = {
    content: ["./src/**/*.{js,jsx,ts,tsx}"],
    theme: {
        screens: {
            sm: "480px",
            md: "768px",
            lg: "976px",
            xl: "1440px",
        },

        fontFamily: {
            sans: ["Roboto", "sans-serif"],
            serif: ["Open Sans", "serif"],
        },
        extend: {
            colors: {
                primary: {
                    DEFAULT: "#6200EE",
                    light: "#9C47FF",
                    dark: "#3700B3",
                },
                secondary: {
                    DEFAULT: "#03DAC6",
                    light: "#6EFFE8",
                    dark: "#018786",
                },
                background: {
                    DEFAULT: "#FFFFFF",
                },
                surface: {
                    DEFAULT: "#FFFFFF",
                },
                error: {
                    DEFAULT: "#B00020",
                },
                onprimary: {
                    DEFAULT: "#FFFFFF",
                },
                onsecondary: {
                    DEFAULT: "#000000",
                },
                onbackground: {
                    DEFAULT: "#000000",
                },
                onsurface: {
                    DEFAULT: "#000000",
                },
                onerror: {
                    DEFAULT: "#FFFFFF",
                },
            },

            outline: {
                primary: "1px solid #6200EE",
                secondary: "1px solid #03DAC6",
            },
        },
    },
    plugins: [],
};
