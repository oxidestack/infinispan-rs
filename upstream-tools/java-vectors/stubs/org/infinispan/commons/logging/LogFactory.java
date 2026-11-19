package org.infinispan.commons.logging;
public class LogFactory {
    public static Log getLog(Class<?> clazz) {
        return new Log() {
            public boolean isTraceEnabled() { return false; }
            public void tracef(String fmt, Object... args) { }
        };
    }
}
