using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Assignment14;

public static class Solve
{
    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(180)
    };
    public const string TopApiUrl = "http://127.0.0.1:8123";
    private static readonly object _treeLock = new object();
    // Increase throttle to allow higher concurrency
    private static readonly SemaphoreSlim _throttle = new SemaphoreSlim(500);

    // This function retrieves JSON from the server
    public static async Task<JObject?> GetDataFromServerAsync(string url)
    {
        const int maxAttempts = 5;
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await _throttle.WaitAsync();
                var jsonString = await HttpClient.GetStringAsync(url);
                if (string.IsNullOrWhiteSpace(jsonString))
                    throw new Exception("Empty response");
                return JObject.Parse(jsonString);
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                Console.WriteLine($"Attempt {attempt} failed fetching {url}: {ex.Message}. Retrying...");
                await Task.Delay(100);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching data from {url}: {ex.Message}");
                return null;
            }
            finally
            {
                _throttle.Release();
            }
        }
        return null;
    }

    // This function takes in a person ID and retrieves a Person object
    // Hint: It can be used in a "new List<Task<Person?>>()" list
    private static async Task<Person?> FetchPersonAsync(long personId)
    {
        if (personId == 0) return null;
        var personJson = await GetDataFromServerAsync($"{TopApiUrl}/person/{personId}");
        return personJson != null ? Person.FromJson(personJson.ToString()) : null;
    }

    // This function takes in a family ID and retrieves a Family object
    // Hint: It can be used in a "new List<Task<Family?>>()" list
    private static async Task<Family?> FetchFamilyAsync(long familyId)
    {
        if (familyId == 0) return null;
        var familyJson = await GetDataFromServerAsync($"{TopApiUrl}/family/{familyId}");
        return familyJson != null ? Family.FromJson(familyJson.ToString()) : null;
    }

    // =======================================================================================================
    public static async Task<bool> DepthFS(long familyId, Tree tree)
    {
        // Note: invalid IDs are zero not null

        var family = await FetchFamilyAsync(familyId);
        if (family == null || family.Id == 0) return false;

        lock (_treeLock)
        {
            if (tree.GetFamily(family.Id) != null)
                return true;
            tree.AddFamily(family);
        }

        var personIds = new List<long>();
        if (family.HusbandId != 0) personIds.Add(family.HusbandId);
        if (family.WifeId != 0) personIds.Add(family.WifeId);
        personIds.AddRange(family.Children.Where(id => id != 0));

        var personTasks = personIds.Select(FetchPersonAsync).ToList();
        var persons = await Task.WhenAll(personTasks);

        foreach (var p in persons.Where(p => p != null && p.Id != 0)!)
        {
            lock (_treeLock)
            {
                if (tree.GetPerson(p.Id) == null)
                    tree.AddPerson(p);
            }
        }

        var calls = new List<Task>();
        foreach (var p in persons.Where(p => p != null)!)
        {
            var parentFam = p!.ParentId;
            if (parentFam != 0)
            {
                bool seen;
                lock (_treeLock)
                {
                    seen = tree.GetFamily(parentFam) != null;
                }
                if (!seen)
                    calls.Add(DepthFS(parentFam, tree));
            }
        }

        if (calls.Count > 0)
            await Task.WhenAll(calls);

        return true;
    }

    // =======================================================================================================
    public static async Task<bool> BreathFS(long startFamilyId, Tree tree)
    {
        // Note: invalid IDs are zero not null

        var visited = new HashSet<long>();
        var toVisit  = new List<long> { startFamilyId };
        visited.Add(startFamilyId);

        while (toVisit.Count > 0)
        {
            // Fetch all families in current wave concurrently
            var famTasks = toVisit.Select(FetchFamilyAsync).ToList();
            var fams     = (await Task.WhenAll(famTasks))
                               .Where(f => f != null && f.Id != 0)
                               .Cast<Family>()
                               .ToList();

            toVisit.Clear();
            var personIds     = new List<long>();
            var nextFamilies  = new List<long>();

            foreach (var fam in fams)
            {
                lock (_treeLock)
                {
                    if (tree.GetFamily(fam.Id) != null)
                        continue;
                    tree.AddFamily(fam);
                }

                if (fam.HusbandId != 0) personIds.Add(fam.HusbandId);
                if (fam.WifeId   != 0) personIds.Add(fam.WifeId);
                personIds.AddRange(fam.Children.Where(id => id != 0));
            }

            // Fetch all persons in current wave
            var pTasks = personIds.Select(FetchPersonAsync).ToList();
            var persons = await Task.WhenAll(pTasks);

            foreach (var p in persons.Where(p => p != null && p.Id != 0)!)
            {
                lock (_treeLock)
                {
                    if (tree.GetPerson(p.Id) == null)
                        tree.AddPerson(p);
                }

                var pf = p!.ParentId;
                if (pf != 0 && !visited.Contains(pf))
                {
                    visited.Add(pf);
                    nextFamilies.Add(pf);
                }
            }

            toVisit = nextFamilies;
        }

        return true;
    }
}
