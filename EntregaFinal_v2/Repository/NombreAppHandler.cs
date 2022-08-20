using System.Data;
using System;
using System.Reflection;

namespace NombreAppHandlers
{
    public static class NombreAppHandler
    {
        //Este método va a devolver el nombre de la App 
        public static string TraerNombreApp()
        {
            AppDomain domain = AppDomain.CurrentDomain;
            string nombre_app = domain.FriendlyName;
            return nombre_app;
        }
    }
}
