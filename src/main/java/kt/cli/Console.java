package kt.cli;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Color;
import org.fusesource.jansi.AnsiConsole;

public class Console {

    public static void initConsole() {
        AnsiConsole.systemInstall();
    }

    public static void printError(String text) {
        System.out.println(red(text));
    }

    public static void print(String text) {
        System.out.print(text);
    }

    public static void print(char text) {
        System.out.print(text);
    }

    public static void moveCursorStart() {
        System.out.print(Ansi.ansi().cursorToColumn(1));
    }

    public static void moveCursorUp() {
        System.out.print(Ansi.ansi().cursorUp(1));
    }

    public static void eraseLine() {
        System.out.print(Ansi.ansi().eraseLine());
    }

    public static void println(String text) {
        System.out.println(text);
    }

    public static void println() {
        System.out.println();
    }

    public static String red(String text) {
        return Ansi.ansi().fgRed().a(text).reset().toString();
    }

    public static String yellow(String text) {
        return Ansi.ansi().fgYellow().a(text).reset().toString();
    }

    public static String bright(String text) {
        return Ansi.ansi().fgBright(Color.DEFAULT).a(text).reset().toString();
    }

    public static String warn(String text) {
        return Ansi.ansi().fgYellow().a(text).reset().toString();
    }

    public static String bold(String text) {
        return Ansi.ansi().bold().a(text).reset().toString();
    }

    public static String green(String text) {
        return Ansi.ansi().fgGreen().a(text).reset().toString();
    }
}
